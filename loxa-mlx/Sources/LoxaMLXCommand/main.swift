import Darwin
import Foundation
import LoxaMLXCore
import LoxaMLXServer

private enum CommandError: Error, LocalizedError {
    case usage(String)
    case unknownCommand(String)
    case unknownGenerateFlag(String)
    case duplicateGenerateFlag(String)
    case missingGenerateFlagValue(String)
    case missingGenerateFlag(String)
    case modelPathMustBeAbsolute(String)
    case invalidMaxTokens(String)

    var errorDescription: String? {
        switch self {
        case .usage(let usage):
            usage
        case .unknownCommand(let command):
            "Unknown command '\(command)'. \(Self.usageText)"
        case .unknownGenerateFlag(let flag):
            "Unknown generate flag '\(flag)'. Expected --model, --prompt, and --max-tokens."
        case .duplicateGenerateFlag(let flag):
            "Generate flag '\(flag)' may be provided only once."
        case .missingGenerateFlagValue(let flag):
            "Generate flag '\(flag)' requires a value."
        case .missingGenerateFlag(let flag):
            "Missing required generate flag '\(flag)'. \(Self.generateUsage)"
        case .modelPathMustBeAbsolute(let path):
            "--model must be an absolute local path; received '\(path)'."
        case .invalidMaxTokens(let value):
            "--max-tokens must be a positive integer; received '\(value)'."
        }
    }

    static let generateUsage =
        "Usage: loxa-mlx generate --model <absolute-path> --prompt <text> --max-tokens <n>"

    static let usageText =
        "Usage: loxa-mlx --version | \(generateUsage) | loxa-mlx serve --model <absolute-path> --host 127.0.0.1 --port <port> --engine-token <token>"
}

private struct GenerateOptions {
    let modelDirectory: URL
    let prompt: String
    let maxTokens: Int
}

@main
private enum LoxaMLXCommand {
    static func main() async {
        do {
            try await run(Array(CommandLine.arguments.dropFirst()))
        } catch {
            write(error.localizedDescription + "\n", to: .standardError)
            Darwin.exit(2)
        }
    }

    private static func run(_ arguments: [String]) async throws {
        guard let command = arguments.first else {
            throw CommandError.usage(CommandError.usageText)
        }

        switch command {
        case "--version":
            guard arguments.count == 1 else {
                throw CommandError.usage("--version accepts no arguments.")
            }
            print(LoxaMLXVersion.current)

        case "generate":
            let options = try parseGenerate(Array(arguments.dropFirst()))
            let runtime = try MLXModelRuntime(modelDirectory: options.modelDirectory)
            try await runtime.load()
            let request = try GenerationRequest(
                requestID: "cli-\(UUID().uuidString)",
                prompt: options.prompt,
                temperature: 0,
                maxTokens: options.maxTokens
            )
            try await runtime.generate(request) { event in
                if case .token(let piece) = event {
                    write(piece, to: .standardOutput)
                }
            }

        case "serve":
            let options = try ServeCommand.parse(Array(arguments.dropFirst()))
            try await ServeCommand.run(options)

        default:
            throw CommandError.unknownCommand(command)
        }
    }

    private static func parseGenerate(_ arguments: [String]) throws -> GenerateOptions {
        let allowedFlags: Set<String> = ["--model", "--prompt", "--max-tokens"]
        var values: [String: String] = [:]
        var index = 0

        while index < arguments.count {
            let flag = arguments[index]
            guard allowedFlags.contains(flag) else {
                throw CommandError.unknownGenerateFlag(flag)
            }
            guard values[flag] == nil else {
                throw CommandError.duplicateGenerateFlag(flag)
            }
            let valueIndex = index + 1
            guard valueIndex < arguments.count else {
                throw CommandError.missingGenerateFlagValue(flag)
            }
            values[flag] = arguments[valueIndex]
            index += 2
        }

        guard let modelPath = values["--model"] else {
            throw CommandError.missingGenerateFlag("--model")
        }
        guard let prompt = values["--prompt"] else {
            throw CommandError.missingGenerateFlag("--prompt")
        }
        guard let maxTokensValue = values["--max-tokens"] else {
            throw CommandError.missingGenerateFlag("--max-tokens")
        }
        guard modelPath.hasPrefix("/") else {
            throw CommandError.modelPathMustBeAbsolute(modelPath)
        }
        guard let maxTokens = Int(maxTokensValue), maxTokens > 0 else {
            throw CommandError.invalidMaxTokens(maxTokensValue)
        }

        return GenerateOptions(
            modelDirectory: URL(fileURLWithPath: modelPath, isDirectory: true),
            prompt: prompt,
            maxTokens: maxTokens
        )
    }

    private static func write(_ text: String, to fileHandle: FileHandle) {
        fileHandle.write(Data(text.utf8))
    }
}
