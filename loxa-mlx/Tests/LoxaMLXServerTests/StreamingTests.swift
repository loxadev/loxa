import Foundation
import XCTest
@testable import LoxaMLXServer

final class StreamingTests: XCTestCase {
    func testRequestParserAcceptsBoundedContentLengthBody() throws {
        let body = Data(#"{"request_id":"req_123","prompt":"Hello","temperature":0,"max_tokens":1}"#.utf8)
        let data = makeRequest(
            requestLine: "POST /generate HTTP/1.1",
            headers: [
                "Authorization: Bearer token",
                "Content-Length: \(body.count)",
                "Content-Type: application/json",
            ],
            body: body
        )

        let request = try HTTPRequestParser.parse(data)

        XCTAssertEqual(request.method, "POST")
        XCTAssertEqual(request.path, "/generate")
        XCTAssertEqual(request.headers["authorization"], "Bearer token")
        XCTAssertEqual(request.body, body)
    }

    func testRequestParserRejectsMalformedRequestLine() {
        let data = makeRequest(requestLine: "GET /health", headers: [], body: Data())

        XCTAssertThrowsError(try HTTPRequestParser.parse(data))
    }

    func testRequestParserRejectsOversizedRequestLine() {
        let path = "/" + String(repeating: "a", count: HTTPRequestParser.maximumRequestLineBytes)
        let data = makeRequest(
            requestLine: "GET \(path) HTTP/1.1",
            headers: [],
            body: Data()
        )

        XCTAssertThrowsError(try HTTPRequestParser.parse(data))
    }

    func testRequestParserRejectsOversizedHeaders() {
        let value = String(repeating: "a", count: HTTPRequestParser.maximumHeaderBytes)
        let data = makeRequest(
            requestLine: "GET /health HTTP/1.1",
            headers: ["X-Oversized: \(value)"],
            body: Data()
        )

        XCTAssertThrowsError(try HTTPRequestParser.parse(data))
    }

    func testRequestParserRejectsOversizedBodyBeforeReadingIt() {
        let data = makeRequest(
            requestLine: "POST /generate HTTP/1.1",
            headers: ["Content-Length: \(HTTPRequestParser.maximumBodyBytes + 1)"],
            body: Data()
        )

        XCTAssertThrowsError(try HTTPRequestParser.parse(data))
    }

    func testRequestParserRejectsChunkedRequestBody() {
        let data = makeRequest(
            requestLine: "POST /generate HTTP/1.1",
            headers: ["Transfer-Encoding: chunked"],
            body: Data("0\r\n\r\n".utf8)
        )

        XCTAssertThrowsError(try HTTPRequestParser.parse(data))
    }

    func testRequestParserRejectsMalformedOrDuplicateContentLength() {
        let malformed = makeRequest(
            requestLine: "POST /generate HTTP/1.1",
            headers: ["Content-Length: one"],
            body: Data()
        )
        let duplicate = makeRequest(
            requestLine: "POST /generate HTTP/1.1",
            headers: ["Content-Length: 0", "Content-Length: 0"],
            body: Data()
        )

        XCTAssertThrowsError(try HTTPRequestParser.parse(malformed))
        XCTAssertThrowsError(try HTTPRequestParser.parse(duplicate))
    }

    func testRequestParserRejectsTruncatedOrTrailingBodyBytes() {
        let truncated = makeRequest(
            requestLine: "POST /generate HTTP/1.1",
            headers: ["Content-Length: 2"],
            body: Data("x".utf8)
        )
        let trailing = makeRequest(
            requestLine: "POST /generate HTTP/1.1",
            headers: ["Content-Length: 0"],
            body: Data("x".utf8)
        )

        XCTAssertThrowsError(try HTTPRequestParser.parse(truncated))
        XCTAssertThrowsError(try HTTPRequestParser.parse(trailing))
    }

    func testRequestParserRejectsMalformedAndDuplicateHeaders() {
        let malformed = makeRequest(
            requestLine: "GET /health HTTP/1.1",
            headers: ["not-a-header"],
            body: Data()
        )
        let duplicateAuthorization = makeRequest(
            requestLine: "GET /health HTTP/1.1",
            headers: [
                "Authorization: Bearer first",
                "Authorization: Bearer second",
            ],
            body: Data()
        )

        XCTAssertThrowsError(try HTTPRequestParser.parse(malformed))
        XCTAssertThrowsError(try HTTPRequestParser.parse(duplicateAuthorization))
    }

    private func makeRequest(
        requestLine: String,
        headers: [String],
        body: Data
    ) -> Data {
        let head = ([requestLine] + headers).joined(separator: "\r\n") + "\r\n\r\n"
        var data = Data(head.utf8)
        data.append(body)
        return data
    }
}
