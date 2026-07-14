import Foundation

public enum LoxaMLXVersion {
    public static let current = "0.1.0"
}

public enum GenerationRequestError: Error, Equatable, LocalizedError {
    case nonPositiveMaxTokens(Int)

    public var errorDescription: String? {
        switch self {
        case .nonPositiveMaxTokens(let value):
            "max_tokens must be positive; received \(value)"
        }
    }
}

public struct GenerationRequest: Codable, Sendable, Equatable {
    public let requestID: String
    public let prompt: String
    public let temperature: Float
    public let maxTokens: Int

    public init(
        requestID: String,
        prompt: String,
        temperature: Float,
        maxTokens: Int
    ) throws {
        guard maxTokens > 0 else {
            throw GenerationRequestError.nonPositiveMaxTokens(maxTokens)
        }

        self.requestID = requestID
        self.prompt = prompt
        self.temperature = temperature
        self.maxTokens = maxTokens
    }

    private enum CodingKeys: String, CodingKey {
        case requestID = "request_id"
        case prompt
        case temperature
        case maxTokens = "max_tokens"
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        try self.init(
            requestID: container.decode(String.self, forKey: .requestID),
            prompt: container.decode(String.self, forKey: .prompt),
            temperature: container.decode(Float.self, forKey: .temperature),
            maxTokens: container.decode(Int.self, forKey: .maxTokens)
        )
    }
}

public struct GenerationUsage: Codable, Sendable, Equatable {
    public let promptTokens: Int
    public let completionTokens: Int

    public init(promptTokens: Int, completionTokens: Int) {
        self.promptTokens = promptTokens
        self.completionTokens = completionTokens
    }

    private enum CodingKeys: String, CodingKey {
        case promptTokens = "prompt_tokens"
        case completionTokens = "completion_tokens"
    }
}

public enum GenerationFinishReason: String, Codable, Sendable {
    case stop
    case length
    case cancelled
    case error
}

public enum GenerationEvent: Sendable, Equatable {
    case started(requestID: String)
    case token(String)
    case usage(GenerationUsage)
    case finished(GenerationFinishReason)
    case error(String)
}

public protocol TextGenerating: Sendable {
    func load() async throws
    func warmUp() async throws
    func generate(
        _ request: GenerationRequest,
        onEvent: @escaping @Sendable (GenerationEvent) async throws -> Void
    ) async throws
}
