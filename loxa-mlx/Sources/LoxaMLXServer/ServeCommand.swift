import Foundation
import LoxaMLXCore

public struct ServeOptions: Sendable, Equatable {
    public let modelDirectory: URL
    public let host: String
    public let port: UInt16
    public let engineToken: String

    public init(
        modelDirectory: URL,
        host: String,
        port: UInt16,
        engineToken: String
    ) {
        self.modelDirectory = modelDirectory
        self.host = host
        self.port = port
        self.engineToken = engineToken
    }
}

public enum ServeCommandError: Error, Sendable, Equatable, LocalizedError {
    case malformedArguments
    case missingOption(String)
    case duplicateOption(String)
    case unknownOption(String)
    case invalidModelPath
    case invalidHost
    case invalidPort
    case invalidEngineToken

    public var errorDescription: String? {
        switch self {
        case .malformedArguments:
            "Serve options must be provided as flag/value pairs"
        case .missingOption(let option):
            "Missing required serve option: \(option)"
        case .duplicateOption(let option):
            "Duplicate serve option: \(option)"
        case .unknownOption(let option):
            "Unknown serve option: \(option)"
        case .invalidModelPath:
            "The model path must be an existing absolute directory"
        case .invalidHost:
            "The serve host must be 127.0.0.1"
        case .invalidPort:
            "The serve port must be in 1...65535"
        case .invalidEngineToken:
            "The engine token is invalid"
        }
    }
}

public enum ServeCommand {
    private static let requiredOptions = [
        "--model",
        "--host",
        "--port",
        "--engine-token",
    ]

    public static func parse(_ arguments: [String]) throws -> ServeOptions {
        guard arguments.count.isMultiple(of: 2) else {
            throw ServeCommandError.malformedArguments
        }

        var values: [String: String] = [:]
        var index = arguments.startIndex
        while index < arguments.endIndex {
            let option = arguments[index]
            let valueIndex = arguments.index(after: index)
            guard requiredOptions.contains(option) else {
                throw ServeCommandError.unknownOption(option)
            }
            guard values[option] == nil else {
                throw ServeCommandError.duplicateOption(option)
            }
            values[option] = arguments[valueIndex]
            index = arguments.index(valueIndex, offsetBy: 1)
        }

        for option in requiredOptions where values[option] == nil {
            throw ServeCommandError.missingOption(option)
        }

        guard let modelPath = values["--model"],
              NSString(string: modelPath).isAbsolutePath
        else {
            throw ServeCommandError.invalidModelPath
        }
        let modelDirectory = URL(fileURLWithPath: modelPath, isDirectory: true)
            .standardizedFileURL
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(
            atPath: modelDirectory.path,
            isDirectory: &isDirectory
        ), isDirectory.boolValue else {
            throw ServeCommandError.invalidModelPath
        }

        guard values["--host"] == "127.0.0.1" else {
            throw ServeCommandError.invalidHost
        }
        guard let portValue = values["--port"],
              let parsedPort = UInt16(portValue),
              parsedPort > 0
        else {
            throw ServeCommandError.invalidPort
        }
        guard let engineToken = values["--engine-token"],
              !engineToken.isEmpty,
              !engineToken.contains("\r"),
              !engineToken.contains("\n")
        else {
            throw ServeCommandError.invalidEngineToken
        }

        return ServeOptions(
            modelDirectory: modelDirectory,
            host: "127.0.0.1",
            port: parsedPort,
            engineToken: engineToken
        )
    }

    public static func run(_ options: ServeOptions) async throws {
        let runtime = try MLXModelRuntime(modelDirectory: options.modelDirectory)
        let coordinator = GenerationCoordinator(generator: runtime)
        let server = HTTPServer(
            host: options.host,
            port: options.port,
            engineToken: options.engineToken,
            coordinator: coordinator
        )

        try await server.start()
        let loadingTask = Task {
            await coordinator.startLoading()
        }
        defer {
            loadingTask.cancel()
            server.stop()
        }

        try await withTaskCancellationHandler {
            try await server.waitUntilStopped()
            try Task.checkCancellation()
        } onCancel: {
            server.stop()
        }
    }
}
