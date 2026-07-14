import Foundation
import LoxaMLXCore

public enum GenerationCoordinatorError: Error, Sendable, Equatable, LocalizedError {
    case notReady
    case generationAlreadyInProgress

    public var errorDescription: String? {
        switch self {
        case .notReady:
            "The model is not ready"
        case .generationAlreadyInProgress:
            "A generation is already in progress"
        }
    }
}
public actor GenerationCoordinator {
    private enum LoadingState {
        case notStarted
        case loading
        case ready
        case failed
    }

    private struct ActiveGeneration {
        let requestID: String
        let task: Task<Void, Error>
    }

    private let generator: any TextGenerating
    private var loadingState = LoadingState.notStarted
    private var activeGeneration: ActiveGeneration?

    public init(generator: any TextGenerating) {
        self.generator = generator
    }

    public func startLoading() async {
        guard case .notStarted = loadingState else {
            return
        }
        loadingState = .loading

        do {
            try await generator.load()
            try await generator.warmUp()
            loadingState = .ready
        } catch {
            loadingState = .failed
        }
    }

    public func isReady() -> Bool {
        if case .ready = loadingState {
            return true
        }
        return false
    }

    public func generate(
        _ request: GenerationRequest,
        emit: @escaping @Sendable (GenerationEvent) async throws -> Void
    ) async throws {
        guard case .ready = loadingState else {
            throw GenerationCoordinatorError.notReady
        }
        guard activeGeneration == nil else {
            throw GenerationCoordinatorError.generationAlreadyInProgress
        }

        let generator = self.generator
        let emission = TerminalEmission(emit: emit)
        let generationTask = Task {
            do {
                try await generator.generate(request) { event in
                    try await emission.forward(event)
                }
            } catch is CancellationError {
                try? await emission.emitCancelledIfWritable()
                throw CancellationError()
            } catch {
                if Task.isCancelled {
                    try? await emission.emitCancelledIfWritable()
                } else {
                    try? await emission.emitErrorIfWritable(error.localizedDescription)
                }
                throw error
            }
        }
        activeGeneration = ActiveGeneration(
            requestID: request.requestID,
            task: generationTask
        )

        do {
            try await generationTask.value
            activeGeneration = nil
        } catch {
            activeGeneration = nil
            throw error
        }
    }

    public func cancel(requestID: String) -> Bool {
        guard let activeGeneration,
              activeGeneration.requestID == requestID
        else {
            return false
        }

        activeGeneration.task.cancel()
        return true
    }
}

private actor TerminalEmission {
    private let emit: @Sendable (GenerationEvent) async throws -> Void
    private var terminalEmitted = false
    private var writable = true

    init(emit: @escaping @Sendable (GenerationEvent) async throws -> Void) {
        self.emit = emit
    }

    func forward(_ event: GenerationEvent) async throws {
        guard !terminalEmitted, writable else {
            return
        }
        if event.isTerminal {
            terminalEmitted = true
        }

        do {
            try await emit(event)
        } catch {
            writable = false
            throw error
        }
    }

    func emitCancelledIfWritable() async throws {
        try await emitTerminalIfWritable(.finished(.cancelled))
    }

    func emitErrorIfWritable(_ message: String) async throws {
        try await emitTerminalIfWritable(.error(message))
    }

    private func emitTerminalIfWritable(_ event: GenerationEvent) async throws {
        guard !terminalEmitted, writable else {
            return
        }
        terminalEmitted = true

        do {
            try await emit(event)
        } catch {
            writable = false
            throw error
        }
    }
}

private extension GenerationEvent {
    var isTerminal: Bool {
        switch self {
        case .finished, .error:
            true
        case .started, .token, .usage:
            false
        }
    }
}
