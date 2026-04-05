import struct Foundation.Data
import struct SystemPackage.Errno

import SnappyC

/// Snappy Compression for Data.
public extension Data {

    // MARK: - Data Compression

    private func _compress() -> (data: Data, error: Int32) {
        var compressedDataLength = snappy_max_compressed_length(self.count)
        var environment = snappy_env()
        snappy_init_env(&environment)
        let result: (buffer: UnsafeMutablePointer<Int8>, error: Int32) = self.withUnsafeBytes {
            let compressedDataBuffer = UnsafeMutablePointer<Int8>.allocate(capacity: compressedDataLength)
            let error = snappy_compress(&environment, $0.baseAddress, self.count, compressedDataBuffer, &compressedDataLength)
            return (compressedDataBuffer, abs(error))
        }
        snappy_free_env(&environment)
        return (Data(bytesNoCopy: result.buffer, count: compressedDataLength, deallocator: .none), result.error)
    }

    /// Compresses the given Data using Snappy.
    ///
    /// This method is high-level. If an error occurs in the compression it will silently fail and return nil.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: The snappy-compressed data or nil on error.
    @available(macOS, deprecated: 11.0, message: "Please use 'Data.compressedUsingSnappy()' instead")
    @available(iOS, deprecated: 14, message: "Please use 'Data.compressedUsingSnappy()' instead")
    @available(tvOS, deprecated: 14, message: "Please use 'Data.compressedUsingSnappy()' instead")
    @available(watchOS, deprecated: 7, message: "Please use 'Data.compressedUsingSnappy()' instead")
    func compressedWithSnappy() -> Data? {
        let result = _compress()
        if result.error != 0 { return nil }
        return result.data
    }

    /// Compresses the given Data using Snappy.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: A result containing the snappy-compressed data or a system error.
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func compressedUsingSnappyWithResult() -> Result<Data, Errno> {
        let (data, error) = _compress()
        if error != 0 {
            return .failure(Errno(rawValue: error))
        }
        return .success(data)
    }

    /// Compresses the given Data using Snappy.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: The snappy-compressed data or a system error.
    /// - Throws: A System Error (Errno).
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func compressedUsingSnappy() throws -> Data {
        let (data, error) = _compress()
        if error != 0 {
            throw Errno(rawValue: error)
        }
        return data
    }

    /// Compresses the given Data asynchronously using Snappy.
    ///
    /// This method is high-level. If an error occurs in the compression it will silently fail and return nil.
    ///
    /// - Returns: The snappy-compressed data or nil on error.
    @available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
    @available(macOS, deprecated: 11.0, message: "Please use 'Data.compressedUsingSnappy()' instead")
    @available(iOS, deprecated: 14, message: "Please use 'Data.compressedUsingSnappy()' instead")
    @available(tvOS, deprecated: 14, message: "Please use 'Data.compressedUsingSnappy()' instead")
    @available(watchOS, deprecated: 7, message: "Please use 'Data.compressedUsingSnappy()' instead")
    func compressedWithSnappy() async -> Data? {
        await withCheckedContinuation { continuation in
            continuation.resume(with: .success(compressedWithSnappy()))
        }
    }

    /// Compresses the given Data asynchronously using Snappy.
    ///
    /// - Returns: The snappy-compressed data or nil on error.
    /// - Throws: A System Error (Errno).
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func compressedUsingSnappy() async throws -> Data {
        try await withCheckedThrowingContinuation { continuation in
            return continuation.resume(with: compressedUsingSnappyWithResult())
        }
    }


    // MARK: - Data Uncompression
    private func _uncompress() -> (data: Data, error: Int32) {
        let uncompressedLength = self.withUnsafeBytes { ptr -> Int in
            var len = 0
            snappy_uncompressed_length(ptr.baseAddress, self.count, &len)
            return len
        }
        let buffer = UnsafeMutablePointer<Int8>.allocate(capacity: uncompressedLength)
        defer { buffer.deallocate() }
        let error = self.withUnsafeBytes {
            abs(snappy_uncompress($0.baseAddress, self.count, buffer))
        }
        guard error == 0 else {
            return (Data(), error)
        }
        return (Data(bytes: buffer, count: uncompressedLength), error)
    }

    /// Uncompresses the given Data using Snappy.
    ///
    /// This method is high-level. If an error occurs in the uncompression it will silently fail and return nil.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: The uncompressed data or nil on error.
    @available(macOS, deprecated: 11.0, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    @available(iOS, deprecated: 14, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    @available(tvOS, deprecated: 14, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    @available(watchOS, deprecated: 7, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    func uncompressedWithSnappy() -> Data? {
        let (data, error) = _uncompress()
        if error != 0 { return nil }
        return data
    }

    /// Compresses the given Data using Snappy.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: A result containing the uncompressed data or a system error.
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func uncompressedUsingSnappyWithResult() -> Result<Data, Errno> {
        let (data, error) = _uncompress()
        if error != 0 {
            return .failure(Errno(rawValue: error))
        }
        return .success(data)
    }

    /// Compresses the given Data using Snappy.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: The uncompressed data.
    /// - Throws: A System Error (Errno).
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func uncompressedUsingSnappy() throws -> Data {
        let (data, error) = _uncompress()
        if error != 0 {
            throw Errno(rawValue: error)
        }
        return data
    }

    /// Uncompresses the given Data asynchronously using Snappy.
    ///
    /// This method is high-level. If an error occurs in the uncompression it will silently fail and return nil.
    ///
    /// - Returns: The uncompressed data or a system error.
    @available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
    @available(macOS, deprecated: 11.0, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    @available(iOS, deprecated: 14, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    @available(tvOS, deprecated: 14, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    @available(watchOS, deprecated: 7, message: "Please use 'Data.uncompressedUsingSnappy()' instead")
    func uncompressedWithSnappy() async -> Data? {
        await withCheckedContinuation { continuation in
            continuation.resume(with: .success(uncompressedWithSnappy()))
        }
    }

    /// Uncompresses the given Data asynchronously using Snappy.
    ///
    /// - Returns: The uncompressed data or a system error.
    /// - Throws: A System Error (Errno).
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func uncompressedUsingSnappy() async throws -> Data {
        try await withCheckedThrowingContinuation { continuation in
            continuation.resume(with: uncompressedUsingSnappyWithResult())
        }
    }
}

/// Snappy Compression for String.
public extension String {

    // MARK: - String Compression

    /// Compresses the given String using Snappy.
    ///
    /// The String will be encoded to Data using UTF8.
    /// If this is not the behavior you want to use,
    /// encode the String to Data by yourself and call ``Data.compressedUsingSnappy()``.
    ///
    /// This method is high-level. If an error occurs in the compression it will silently fail and return nil.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: The snappy-compressed data or nil on error.
    @available(macOS, deprecated: 11.0, message: "Please use 'String.compressedUsingSnappy()' instead")
    @available(iOS, deprecated: 14, message: "Please use 'String.compressedUsingSnappy()' instead")
    @available(tvOS, deprecated: 14, message: "Please use 'String.compressedUsingSnappy()' instead")
    @available(watchOS, deprecated: 7, message: "Please use 'String.compressedUsingSnappy()' instead")
    func compressedWithSnappy() -> Data? {
        self.data(using: .utf8)?.compressedWithSnappy()
    }

    /// Compresses the given Data asynchronously using Snappy.
    ///
    /// The String will be encoded to Data using UTF8.
    /// If this is not the behavior you want to use,
    /// encode the String to Data by yourself and call ``Data.compressedUsingSnappy()``.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: A result containing the snappy-compressed data or a system error. If the string could not be encoded to data, nil will be returned.
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func compressedUsingSnappyWithResult() -> Result<Data, Errno>? {
        self.data(using: .utf8)?.compressedUsingSnappyWithResult()
    }

    /// Compresses the given Data asynchronously using Snappy.
    ///
    /// The String will be encoded to Data using UTF8.
    /// If this is not the behavior you want to use,
    /// encode the String to Data by yourself and call ``Data.compressedUsingSnappy()``.
    ///
    /// - Warning: This method will block the thread it is executed on.
    ///
    /// - Returns: The snappy-compressed data or a system error. If the string could not be encoded to data, nil will be returned.
    /// - Throws: A System Error (Errno).
    @available(macOS 11.0, iOS 14, tvOS 14, watchOS 7, *)
    func compressedUsingSnappy() throws -> Data? {
        guard let result: Result<Data, Errno> = self.data(using: .utf8)?.compressedUsingSnappyWithResult() else { return nil }
        switch result {
        case .success(let data):
            return data
        case .failure(let error):
            throw error
        }
    }

    /// Compresses the given String asynchronously using Snappy.
    ///
    /// The String will be encoded to Data using UTF8.
    /// If this is not the behavior you want to use,
    /// encode the String to Data by yourself and call ``Data.compressedUsingSnappy()``.
    ///
    /// This method is high-level. If an error occurs in the compression it will silently fail and return nil.
    ///
    /// - Returns: The snappy-compressed data or nil on error.
    @available(macOS 10.15, iOS 13, tvOS 13, watchOS 6, *)
    @available(macOS, deprecated: 11.0, message: "Please use 'String.compressedUsingSnappy() async throws -> Data' instead")
    @available(iOS, deprecated: 14, message: "Please use 'String.compressedUsingSnappy() async throws -> Data' instead")
    @available(tvOS, deprecated: 14, message: "Please use 'String.compressedUsingSnappy() async throws -> Data' instead")
    @available(watchOS, deprecated: 7, message: "Please use 'String.compressedUsingSnappy() async throws -> Data' instead")
    func compressedWithSnappy() async -> Data? {
        await withCheckedContinuation { continuation in
            continuation.resume(with: .success(compressedWithSnappy()))
        }
    }

    /// Compresses the given String asynchronously using Snappy.
    ///
    /// The String will be encoded to Data using UTF8.
    /// If this is not the behavior you want to use,
    /// encode the String to Data by yourself and call ``Data.compressedUsingSnappy()``.
    ///
    /// This method is high-level. If an error occurs in the compression it will silently fail and return nil.
    ///
    /// - Returns: The snappy-compressed data or nil, if the string could not be encoded to data.
    /// - Throws: A System Error (Errno).
    @available(macOS 11, iOS 14, tvOS 14, watchOS 7, *)
    func compressedUsingSnappy() async throws -> Data? {
        try await withCheckedThrowingContinuation { continuation in
            guard let result = compressedUsingSnappyWithResult() else {
                continuation.resume(returning: nil)
                return
            }
            switch result {
            case .success(let data):
                continuation.resume(returning: data)
            case .failure(let error):
                continuation.resume(throwing: error)
            }
        }
    }
}
