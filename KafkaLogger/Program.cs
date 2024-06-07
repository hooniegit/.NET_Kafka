using System;
using System.IO;
using Microsoft.Extensions.Logging;

/**
    - Log Manually
    - Log Tasks
    - Write Logs to Files
*/

public class LogManager
{
    private readonly string _baseDirectory;

    // Initialize
    public LogManager(string baseDirectory)
    {
        _baseDirectory = baseDirectory;
    }

    // Basic
    private void WriteLog(string directory, LogLevel level, string message)
    {
        var logDirectory = Path.Combine(_baseDirectory, directory);
        Directory.CreateDirectory(logDirectory);

        var logFilePath = Path.Combine(logDirectory, $"{DateTime.Now:yyyyMMdd}.log");
        using (var writer = new StreamWriter(logFilePath, true))
        {
            writer.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{level}] {message}");
        }
    }

    // Reference
    public void Log(string directory, LogLevel level, string message)
    {
        WriteLog(directory, level, message);
    }

    // Task Logging
    public void ExecuteWithLogging(string directory, Action action, LogLevel level = LogLevel.Information, string successMessage = "Operation completed successfully.", string errorMessage = "An error occurred.")
    {
        try
        {
            // Run Functions & Log Success Message
            action();
            Log(directory, level, successMessage);
        }
        catch (Exception ex)
        {
            // 
            LogError(directory, ex, errorMessage);
            throw;
        }
    }

    // Manual Logging with Levels
    public void LogInformation(string directory, string message)
    {
        Log(directory, LogLevel.Information, message);
    }

    public void LogWarning(string directory, string message)
    {
        Log(directory, LogLevel.Warning, message);
    }

    public void LogError(string directory, string message)
    {
        Log(directory, LogLevel.Error, message);
    }

    public void LogCritical(string directory, string message)
    {
        Log(directory, LogLevel.Critical, message);
    }
    
    // Exception Usage
    public void LogError(string directory, Exception exception, string message)
    {
        Log(directory, LogLevel.Error, $"{message} Exception: {exception}");
    }

    public void LogCritical(string directory, Exception exception, string message)
    {
        Log(directory, LogLevel.Critical, $"{message} Exception: {exception}");
    }
}

public enum LogLevel
{
    Information,
    Warning,
    Error,
    Critical
}


public class Program
{
    // Define LogManager
    private readonly LogManager _logManager;

    // Initialization
    public Program(LogManager logManager)
    {
        _logManager = logManager;
    }

    public void Run()
    {
        var manualLogDirectory = "Manual";
        var taskLogDirectory = "TaskName";

        // Log Message Manually
        _logManager.LogInformation(manualLogDirectory, "[Manual] Application started.");

        // Run Tasks with Logs
        _logManager.ExecuteWithLogging(taskLogDirectory, () =>
        {
            _logManager.LogInformation(taskLogDirectory, "[Task] Task started.");
            
            throw new InvalidOperationException("Sample Exception");
        }, LogLevel.Information, "[Task] Operation succeeded.", "[Task] Operation failed.");

        // Log Message Manually
        _logManager.LogInformation(manualLogDirectory, "[Manual] Application ended.");
    }

    public static void Main(string[] args)
    {
        var logManager = new LogManager("C:\\Users\\dhkim\\Desktop\\Logs");

        var program = new Program(logManager);
        program.Run();
    }
}
