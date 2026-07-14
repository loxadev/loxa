import Foundation
import XCTest

@testable import LoxaMLXCore

final class ModelPathTests: XCTestCase {
    func testMissingModelDirectoryIsRejected() {
        let missingURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)

        XCTAssertThrowsError(try MLXModelRuntime(modelDirectory: missingURL))
    }

    func testRegularFileModelPathIsRejected() throws {
        let fileURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: false)
        try Data().write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        XCTAssertThrowsError(try MLXModelRuntime(modelDirectory: fileURL))
    }

    func testExistingAbsoluteModelDirectoryIsAccepted() throws {
        let directoryURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(at: directoryURL) }

        XCTAssertTrue(directoryURL.path.hasPrefix("/"))
        XCTAssertNoThrow(try MLXModelRuntime(modelDirectory: directoryURL))
    }
}
