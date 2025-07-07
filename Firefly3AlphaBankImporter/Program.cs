// Required NuGet packages:
// <PackageReference Include="System.CommandLine" Version="2.0.0-beta4.22272.1" />
// <PackageReference Include="Microsoft.Data.Sqlite" Version="7.0.0" />

using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

class Program
{
    static int Main(string[] args)
    {
        var rootCommand = new RootCommand("Import CSV transactions into Firefly III via API");

        var csvOption = new Option<FileInfo>("--csv", "-c")
        {
            Description = "Path to CSV file",
            Required = true
        };

        var dbOption = new Option<FileInfo>("--db", "-d")
        {
            Description = "SQLite DB file",
            DefaultValueFactory = _ => new FileInfo("transactions.db")
        };

        var urlOption = new Option<string>("--url", "-u")
        {
            Description = "Firefly API base URL",
            DefaultValueFactory = _ => Environment.GetEnvironmentVariable("FIREFLY_URL")
        };

        var tokenOption = new Option<string>("--token", "-t")
        {
            Description = "Firefly API token",
            DefaultValueFactory = _ => Environment.GetEnvironmentVariable("FIREFLY_TOKEN")
        };

        var bankOption = new Option<string>("--bank-account", "-b")
        {
            Description = "Firefly bank account ID",
            DefaultValueFactory = _ => Environment.GetEnvironmentVariable("FIREFLY_BANK_ACCOUNT_ID")
        };

        var cashOption = new Option<string>("--cash-account", "-h")
        {
            Description = "Firefly cash account ID",
            DefaultValueFactory = _ => Environment.GetEnvironmentVariable("FIREFLY_CASH_ACCOUNT_ID")
        };

        rootCommand.Options.Add(csvOption);
        rootCommand.Options.Add(dbOption);
        rootCommand.Options.Add(urlOption);
        rootCommand.Options.Add(tokenOption);
        rootCommand.Options.Add(bankOption);
        rootCommand.Options.Add(cashOption);

        var parseResult = rootCommand.Parse(args);

        var csvFile = parseResult.GetValue(csvOption);
        var dbFile = parseResult.GetValue(dbOption);
        var url = parseResult.GetValue(urlOption);
        var token = parseResult.GetValue(tokenOption);
        var bankAccount = parseResult.GetValue(bankOption);
        var cashAccount = parseResult.GetValue(cashOption);

        var existingHashes = FetchExistingHashes(
            new HttpClient {BaseAddress = new Uri(url), DefaultRequestHeaders = {{"Authorization", $"Bearer {token}"}}}
        );

        Run(csvFile, dbFile, url, token, bankAccount, cashAccount, existingHashes);

        return 0;
    }

    static void Run(FileInfo csv, FileInfo db, string url, string token, string bankAccount, string cashAccount, HashSet<string> existingHashes)
    {
        if (string.IsNullOrEmpty(url) || string.IsNullOrEmpty(token)
                                      || string.IsNullOrEmpty(bankAccount) || string.IsNullOrEmpty(cashAccount))
        {
            Console.Error.WriteLine("Error: API URL, token, and both account IDs must be provided.");
            Environment.Exit(1);
        }

        using var conn = InitDb(db.FullName);
        int processed = 0, skipped = 0;

        using var client = new HttpClient();
        client.BaseAddress = new Uri(url);
        client.DefaultRequestHeaders.Add("Authorization", $"Bearer {token}");

        using var reader = new StreamReader(csv.FullName, Encoding.UTF8);
        var header = reader.ReadLine()?.Split(',');
        if (header == null)
        {
            Console.Error.WriteLine("CSV is empty or invalid");
            return;
        }

        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (string.IsNullOrWhiteSpace(line)) continue;

            var fields = SplitCsvLine(line);
            var row = header.Zip(fields, (h, v) => new {h, v})
                .ToDictionary(x => x.h.Trim(), x => x.v.Trim());

            // 1) Всегда проверяем дату проводки
            if (!row.TryGetValue("transactionDate", out var date)
                || string.IsNullOrEmpty(date))
            {
                skipped++;
                continue;
            }

            // 2) Только для не-пополнений проверяем статус
            row.TryGetValue("type", out var typeValue);
            if (!string.Equals(typeValue, "Пополнение", StringComparison.OrdinalIgnoreCase))
            {
                if (!row.TryGetValue("status", out var status)
                    || status != "Выполнен")
                {
                    skipped++;
                    continue;
                }
            }

            // 3) Вычисляем хэш, проверяем дубли и шлём транзакцию
            var txnId = ComputeUniqueId(row.Values);
            if (AlreadyProcessed(conn, txnId) || existingHashes.Contains(txnId))
            {
                skipped++;
                continue;
            }

            if (PostTransaction(client, bankAccount, cashAccount, row))
            {
                MarkProcessed(conn, txnId, row);
                processed++;
            }
            else
            {
                skipped++;
            }
        }


        Console.WriteLine($"Done. Processed: {processed}, Skipped: {skipped}");
    }

    static HashSet<string> FetchExistingHashes(HttpClient client)
    {
        var hashes = new HashSet<string>();
        var response = client.GetAsync("/api/v1/transactions?limit=1000").Result;
        if (!response.IsSuccessStatusCode) return hashes;

        using var doc = JsonDocument.Parse(response.Content.ReadAsStringAsync().Result);
        if (doc.RootElement.TryGetProperty("data", out var data))
        {
            foreach (var item in data.EnumerateArray())
            {
                if (item.GetProperty("attributes").TryGetProperty("transactions", out var txs))
                    foreach (var tx in txs.EnumerateArray())
                        if (tx.TryGetProperty("import_hash_v2", out var hashProp)
                            && hashProp.ValueKind == JsonValueKind.String)
                            hashes.Add(hashProp.GetString());
            }
        }

        return hashes;
    }

    static SqliteConnection InitDb(string path)
    {
        var conn = new SqliteConnection(new SqliteConnectionStringBuilder {DataSource = path}.ToString());
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL
        );";
        cmd.ExecuteNonQuery();
        return conn;
    }

    static bool AlreadyProcessed(SqliteConnection conn, string id)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1 FROM transactions WHERE id = $id";
        cmd.Parameters.AddWithValue("$id", id);
        using var reader = cmd.ExecuteReader();
        return reader.Read();
    }

    static void MarkProcessed(SqliteConnection conn, string id, Dictionary<string, string> data)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO transactions (id, data) VALUES ($id, $data)";
        cmd.Parameters.AddWithValue("$id", id);
        cmd.Parameters.AddWithValue("$data", JsonSerializer.Serialize(data));
        cmd.ExecuteNonQuery();
    }

    static string ComputeUniqueId(IEnumerable<string> values)
    {
        using var md5 = MD5.Create();
        var concat = string.Concat(values);
        var hash = md5.ComputeHash(Encoding.UTF8.GetBytes(concat));
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }

    static bool PostTransaction(HttpClient client, string bankAccount, string cashAccount, Dictionary<string, string> row)
    {
        var dateStr = row["transactionDate"];
        // Parse date from CSV to ISO 8601
        if (!DateTimeOffset.TryParse(dateStr, CultureInfo.GetCultureInfo("ru-RU"), DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            Console.Error.WriteLine($"Invalid date format: {dateStr}");
            return false;
        }

        var isoDate = parsedDate.ToString("yyyy-MM-ddTHH:mm:sszzz");

        var amountStr = row["amount"].Replace(',', '.');
        if (!decimal.TryParse(amountStr, NumberStyles.Any, CultureInfo.InvariantCulture, out var amount))
            return false;

        var txnType = row.TryGetValue("type", out var t) && t == "Пополнение" ? "deposit" : "withdrawal";
        var sourceId = txnType == "withdrawal" ? bankAccount : cashAccount;
        var destId = txnType == "deposit" ? bankAccount : cashAccount;
        var description = row.GetValueOrDefault("merchant", string.Empty);

        var payload = new
        {
            apply_rules = true,
            fire_webhooks = true,
            transactions = new[]
            {
                new
                {
                    date = isoDate,
                    type = txnType,
                    description = description,
                    amount = amount,
                    currency = row.GetValueOrDefault("currency", string.Empty),
                    source_id = sourceId,
                    destination_id = destId,
                    category_name = row.GetValueOrDefault("category", string.Empty),
                }
            }
        };

        var request = new HttpRequestMessage(HttpMethod.Post, "/api/v1/transactions")
        {
            Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
        };

        // Required headers for Firefly III API
        request.Headers.Accept.Clear();
        request.Headers.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/vnd.api+json"));
        request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");

        var resp = client.SendAsync(request).Result;
        if (resp.IsSuccessStatusCode)
        {
            Console.WriteLine($"Успешно добавили запись на {(txnType == "deposit" ? "+" : "-")}{amount} руб. ({description})");
            return true;
        }
        Console.Error.WriteLine($"Failed (Status {resp.StatusCode}): {resp.Content.ReadAsStringAsync().Result}");
        return false;
    }

    static string[] SplitCsvLine(string line)
    {
        // Простая разбивка по запятым, без поддержки кавычек
        return line.Split(',');
    }
}
