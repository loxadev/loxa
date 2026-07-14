import Foundation

struct HTTPRequest: Sendable, Equatable {
    let method: String
    let path: String
    let headers: [String: String]
    let body: Data
}

enum HTTPRequestParsingError: Error, Equatable {
    case incomplete
    case malformed
    case requestLineTooLarge
    case headersTooLarge
    case tooManyHeaders
    case bodyTooLarge
    case unsupportedTransferEncoding
}

enum HTTPRequestParser {
    static let maximumRequestLineBytes = 4_096
    static let maximumHeaderBytes = 16_384
    static let maximumHeaderCount = 100
    static let maximumBodyBytes = 1_048_576

    private static let headerTerminator = Data("\r\n\r\n".utf8)
    private static let lineTerminator = Data("\r\n".utf8)

    static func parse(_ data: Data) throws -> HTTPRequest {
        try rejectOversizedIncompleteHead(data)

        guard let headerRange = data.range(of: headerTerminator) else {
            throw HTTPRequestParsingError.incomplete
        }

        let headerByteCount = data.distance(
            from: data.startIndex,
            to: headerRange.lowerBound
        )
        guard headerByteCount <= maximumHeaderBytes else {
            throw HTTPRequestParsingError.headersTooLarge
        }

        let headData = Data(data[..<headerRange.lowerBound])
        guard headData.allSatisfy({ $0 < 128 }),
              let head = String(data: headData, encoding: .utf8)
        else {
            throw HTTPRequestParsingError.malformed
        }

        let lines = head.components(separatedBy: "\r\n")
        guard let requestLine = lines.first, !requestLine.isEmpty else {
            throw HTTPRequestParsingError.malformed
        }
        guard requestLine.utf8.count <= maximumRequestLineBytes else {
            throw HTTPRequestParsingError.requestLineTooLarge
        }

        let requestParts = requestLine.split(
            separator: " ",
            omittingEmptySubsequences: false
        )
        guard requestParts.count == 3,
              !requestParts[0].isEmpty,
              requestParts[1].first == "/",
              requestParts[2] == "HTTP/1.1",
              isHTTPToken(requestParts[0].utf8)
        else {
            throw HTTPRequestParsingError.malformed
        }

        let headerLines = lines.dropFirst()
        guard headerLines.count <= maximumHeaderCount else {
            throw HTTPRequestParsingError.tooManyHeaders
        }

        var headers: [String: String] = [:]
        for line in headerLines {
            guard !line.isEmpty,
                  line.first != " ",
                  line.first != "\t",
                  let colon = line.firstIndex(of: ":"),
                  colon != line.startIndex
            else {
                throw HTTPRequestParsingError.malformed
            }

            let name = line[..<colon]
            let rawValue = line[line.index(after: colon)...]
            guard isHTTPToken(name.utf8),
                  rawValue.utf8.allSatisfy(isValidHeaderValueByte)
            else {
                throw HTTPRequestParsingError.malformed
            }

            let normalizedName = name.lowercased()
            guard headers[normalizedName] == nil else {
                throw HTTPRequestParsingError.malformed
            }
            headers[normalizedName] = rawValue.trimmingCharacters(in: .whitespaces)
        }

        if headers["transfer-encoding"] != nil {
            throw HTTPRequestParsingError.unsupportedTransferEncoding
        }

        let contentLength = try parseContentLength(headers["content-length"])
        guard contentLength <= maximumBodyBytes else {
            throw HTTPRequestParsingError.bodyTooLarge
        }

        let body = Data(data[headerRange.upperBound...])
        if body.count < contentLength {
            throw HTTPRequestParsingError.incomplete
        }
        guard body.count == contentLength else {
            throw HTTPRequestParsingError.malformed
        }

        return HTTPRequest(
            method: String(requestParts[0]),
            path: String(requestParts[1]),
            headers: headers,
            body: body
        )
    }

    private static func rejectOversizedIncompleteHead(_ data: Data) throws {
        if let firstLineRange = data.range(of: lineTerminator) {
            let requestLineByteCount = data.distance(
                from: data.startIndex,
                to: firstLineRange.lowerBound
            )
            guard requestLineByteCount <= maximumRequestLineBytes else {
                throw HTTPRequestParsingError.requestLineTooLarge
            }
        } else if data.count > maximumRequestLineBytes {
            throw HTTPRequestParsingError.requestLineTooLarge
        }

        guard data.range(of: headerTerminator) != nil
                || data.count <= maximumHeaderBytes + headerTerminator.count
        else {
            throw HTTPRequestParsingError.headersTooLarge
        }
    }

    private static func parseContentLength(_ value: String?) throws -> Int {
        guard let value else {
            return 0
        }
        guard !value.isEmpty,
              value.utf8.allSatisfy({ (48...57).contains($0) }),
              let length = Int(value)
        else {
            throw HTTPRequestParsingError.malformed
        }
        return length
    }

    private static func isHTTPToken<Bytes: Collection>(_ bytes: Bytes) -> Bool
    where Bytes.Element == UInt8 {
        !bytes.isEmpty && bytes.allSatisfy { byte in
            switch byte {
            case 48...57, 65...90, 97...122:
                true
            case 33, 35, 36, 37, 38, 39, 42, 43, 45, 46, 94, 95, 96, 124, 126:
                true
            default:
                false
            }
        }
    }

    private static func isValidHeaderValueByte(_ byte: UInt8) -> Bool {
        byte == 9 || (32...126).contains(byte)
    }
}
