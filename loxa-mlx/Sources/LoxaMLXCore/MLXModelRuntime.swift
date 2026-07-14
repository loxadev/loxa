import Foundation
import HuggingFace
import MLX
import MLXHuggingFace
import MLXLLM
import MLXLMCommon
import Tokenizers

public enum MLXModelRuntimeError: Error, Equatable, LocalizedError {
    case modelPathMustBeAbsolute(String)
    case modelDirectoryNotFound(String)
    case modelPathIsNotDirectory(String)
    case modelNotLoaded
    case generationAlreadyInProgress
    case unsupportedToolCall
    case generationEndedWithoutCompletionInfo

    public var errorDescription: String? {
        switch self {
        case .modelPathMustBeAbsolute(let path):
            "Model path must be an absolute local path: \(path)"
        case .modelDirectoryNotFound(let path):
            "Model directory does not exist: \(path)"
        case .modelPathIsNotDirectory(let path):
            "Model path is not a directory: \(path)"
        case .modelNotLoaded:
            "Model is not loaded; call load() before warmUp() or generate()"
        case .generationAlreadyInProgress:
            "Only one generation may run at a time"
        case .unsupportedToolCall:
            "Tool-call generation is outside the text-only Loxa MLX contract"
        case .generationEndedWithoutCompletionInfo:
            "MLX generation ended without completion metadata"
        }
    }
}

public actor MLXModelRuntime: TextGenerating {
    private let modelDirectory: URL
    private var modelContainer: ModelContainer?
    private var isGenerating = false

    public init(modelDirectory: URL) throws {
        guard modelDirectory.isFileURL, modelDirectory.path.hasPrefix("/") else {
            throw MLXModelRuntimeError.modelPathMustBeAbsolute(modelDirectory.relativeString)
        }

        let standardizedURL = modelDirectory.standardizedFileURL
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(
            atPath: standardizedURL.path,
            isDirectory: &isDirectory
        ) else {
            throw MLXModelRuntimeError.modelDirectoryNotFound(standardizedURL.path)
        }
        guard isDirectory.boolValue else {
            throw MLXModelRuntimeError.modelPathIsNotDirectory(standardizedURL.path)
        }

        self.modelDirectory = standardizedURL.resolvingSymlinksInPath()
    }

    public func load() async throws {
        guard modelContainer == nil else {
            return
        }

        modelContainer = try await LLMModelFactory.shared.loadContainer(
            from: modelDirectory,
            using: #huggingFaceTokenizerLoader()
        )
    }

    public func warmUp() async throws {
        try Task.checkCancellation()
        let request = try GenerationRequest(
            requestID: "warm-up",
            prompt: "Hello",
            temperature: 0,
            maxTokens: 1
        )
        try await generate(request) { _ in }
        try Task.checkCancellation()
    }

    public func generate(
        _ request: GenerationRequest,
        onEvent: @escaping @Sendable (GenerationEvent) async throws -> Void
    ) async throws {
        guard !isGenerating else {
            throw MLXModelRuntimeError.generationAlreadyInProgress
        }
        isGenerating = true
        defer { isGenerating = false }

        var terminalAttempted = false

        do {
            try await onEvent(.started(requestID: request.requestID))

            guard let modelContainer else {
                throw MLXModelRuntimeError.modelNotLoaded
            }

            if Task.isCancelled {
                terminalAttempted = true
                try await onEvent(.finished(.cancelled))
                return
            }

            let input = try await modelContainer.prepare(
                input: UserInput(chat: [.user(request.prompt)])
            )
            let stream = try await modelContainer.generate(
                input: input,
                parameters: GenerateParameters(
                    maxTokens: request.maxTokens,
                    temperature: request.temperature
                )
            )

            for await generation in stream {
                switch generation {
                case .chunk(let piece):
                    if Task.isCancelled {
                        terminalAttempted = true
                        try await onEvent(.finished(.cancelled))
                        return
                    }

                    if !piece.isEmpty {
                        try await onEvent(.token(piece))
                    }

                    if Task.isCancelled {
                        terminalAttempted = true
                        try await onEvent(.finished(.cancelled))
                        return
                    }

                case .info(let info):
                    try await onEvent(
                        .usage(
                            GenerationUsage(
                                promptTokens: info.promptTokenCount,
                                completionTokens: info.generationTokenCount
                            )
                        )
                    )
                    terminalAttempted = true
                    try await onEvent(.finished(Self.finishReason(for: info.stopReason)))
                    return

                case .toolCall:
                    throw MLXModelRuntimeError.unsupportedToolCall
                }
            }

            throw MLXModelRuntimeError.generationEndedWithoutCompletionInfo
        } catch is CancellationError {
            if !terminalAttempted {
                terminalAttempted = true
                try? await onEvent(.finished(.cancelled))
            }
        } catch {
            if !terminalAttempted {
                terminalAttempted = true
                try? await onEvent(.error(error.localizedDescription))
            }
            throw error
        }
    }

    private static func finishReason(for reason: GenerateStopReason) -> GenerationFinishReason {
        switch reason {
        case .stop:
            .stop
        case .length:
            .length
        case .cancelled:
            .cancelled
        }
    }
}
