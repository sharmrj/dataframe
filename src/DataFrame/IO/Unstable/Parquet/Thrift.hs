{-# LANGUAGE ApplicativeDo #-}

module DataFrame.IO.Unstable.Parquet.Thrift (parseFileMetadata) where

import Control.Monad (void)
import Data.Bits (shiftL, (.|.))
import qualified Data.ByteString as BS
import Data.Functor ((<&>))
import Data.Int (Int32, Int8)
import Data.List (foldl')
import qualified Data.Text as T
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types (
    ColumnOrder (COLUMN_ORDER_UNKNOWN, TYPE_ORDER),
    ColumnStatistics (..),
    EncryptionAlgorithm (..),
    LogicalType (..),
    PageEncodingStats (..),
    SizeStatistics (..),
    SortingColumn (..),
    TimeUnit (..),
    compressionCodecFromInt,
    emptyColumnStatistics,
    emptyGeospatialStatistics,
    emptyPageEncodingStats,
    emptySizeStatistics,
    emptySortingColumn,
    pageTypeFromInt,
    parquetEncodingFromInt,
    parquetTypeFromInt,
    repetitionTypeFromInt,
 )
import DataFrame.IO.Unstable.Parquet.Reader (
    ParquetReader (..),
    ParquetReaderException (..),
    byte,
    int32,
    int64,
    runParquetReader,
    throwParseError,
 )
import DataFrame.IO.Unstable.Parquet.Thrift.Compact (
    parseStruct,
    skipField,
    thriftBinary,
    thriftList,
    thriftString,
 )
import DataFrame.IO.Utils.RandomAccess (RandomAccess (..))

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1277
fileMetadata :: ParquetReader FileMetadata
fileMetadata =
    parseStruct defaultMetadata $ \filemetadata fieldId compacttype ->
        case fieldId of
            1 -> do v <- int32; return filemetadata{version = v}
            2 -> do sch <- thriftList schemaElement; return filemetadata{schema = sch}
            3 -> do n <- int64; return filemetadata{numRows = fromIntegral n}
            4 -> do rg <- thriftList rowGroup; return filemetadata{rowGroups = rg}
            5 -> do kv <- thriftList keyValuePair; return filemetadata{keyValueMetadata = kv}
            6 -> do s <- thriftString; return filemetadata{createdBy = Just s}
            7 -> do co <- thriftList columnOrder; return filemetadata{columnOrders = co}
            8 -> do
                enc <- parseEncryptionAlgorithm; return filemetadata{encryptionAlgorithm = enc}
            9 -> do bs <- thriftBinary; return filemetadata{footerSigningKeyMetadata = bs}
            _ -> skipField compacttype >> return filemetadata

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L505
schemaElement :: ParquetReader SchemaElement
schemaElement =
    parseStruct defaultSchemaElement $ \schemaelement fieldid compacttype ->
        case fieldid of
            1 -> do t <- int32; return schemaelement{elementType = toIntegralType t}
            2 -> do l <- int32; return schemaelement{typeLength = l}
            3 -> do r <- int32; return schemaelement{repetitionType = repetitionTypeFromInt r}
            4 -> do s <- thriftString; return schemaelement{elementName = T.pack s}
            5 -> do n <- int32; return schemaelement{numChildren = n}
            6 -> do c <- int32; return schemaelement{convertedType = c}
            7 -> do s <- int32; return schemaelement{scale = s}
            8 -> do p <- int32; return schemaelement{precision = p}
            9 -> do f <- int32; return schemaelement{fieldId = f}
            10 -> do lt <- parseLogicalType; return schemaelement{logicalType = lt}
            _ -> skipField compacttype >> return schemaelement

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1021
rowGroup :: ParquetReader RowGroup
rowGroup =
    parseStruct emptyRowGroup $ \rowgroup fieldid compacttype ->
        case fieldid of
            1 -> do cols <- thriftList columnChunk; return rowgroup{rowGroupColumns = cols}
            2 -> do t <- int64; return rowgroup{totalByteSize = t}
            3 -> do n <- int64; return rowgroup{rowGroupNumRows = n}
            4 -> do sc <- thriftList sortingColumn; return rowgroup{rowGroupSortingColumns = sc}
            5 -> do o <- int64; return rowgroup{fileOffset = o}
            6 -> do c <- int64; return rowgroup{totalCompressedSize = c}
            7 -> do o <- int64; return rowgroup{ordinal = fromIntegral o}
            _ -> skipField compacttype >> return rowgroup

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L963
columnChunk :: ParquetReader ColumnChunk
columnChunk =
    parseStruct emptyColumnChunk $ \columnchunk fieldid compacttype ->
        case fieldid of
            -- From the parquet.thrift file. Copy pasted here for convenient reference:
            --  File where column data is stored.  If not set, assumed to be same file as
            --  metadata.  This path is relative to the current file.
            --
            --  As of December 2025, the only known use-case for this field is writing summary
            --  parquet files (i.e. "_metadata" files).  These files consolidate footers from
            --  multiple parquet files to allow for efficient reading of footers to avoid file
            --  listing costs and prune out files that do not need to be read based on statistics.
            --
            --  These files do not appear to have ever been formally specified in the specification.
            --  and are potentially problematic from a correctness perspective [1].
            --
            --  [1] https://lists.apache.org/thread/ootf2kmyg3p01b1bvplpvp4ftd1bt72d
            --
            --  There is no other known usage of this field. Specifically, there are no known
            --  reference implementations that will read externally stored column data if this field is populated
            --  within a standard parquet file. Making use of the field for this purpose is
            --  not considered part of the Parquet specification.
            1 -> do p <- thriftString; return columnchunk{columnChunkFilePath = p}
            2 -> do o <- int64; return columnchunk{columnChunkMetadataFileOffset = o}
            3 -> do m <- parseColumnMetaData; return columnchunk{columnMetaData = m}
            4 -> do o <- int64; return columnchunk{columnChunkOffsetIndexOffset = o}
            5 -> do l <- int32; return columnchunk{columnChunkOffsetIndexLength = l}
            6 -> do o <- int64; return columnchunk{columnChunkColumnIndexOffset = o}
            7 -> do l <- int32; return columnchunk{columnChunkColumnIndexLength = l}
            -- 8 (column crypto metadata) and 9 (encrypted column crypto metadata) are unimplemented
            -- TODO:
            _ -> skipField compacttype >> return columnchunk

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L880
parseColumnMetaData :: ParquetReader ColumnMetaData
parseColumnMetaData =
    parseStruct emptyColumnMetadata $ \columnmetadata fieldid compacttype ->
        case fieldid of
            1 -> do t <- int32; return columnmetadata{columnType = parquetTypeFromInt t}
            2 -> do
                enc <- thriftList (parquetEncodingFromInt <$> int32)
                return columnmetadata{columnEncodings = enc}
            3 -> do p <- thriftList thriftString; return columnmetadata{columnPathInSchema = p}
            4 -> do c <- int32; return columnmetadata{columnCodec = compressionCodecFromInt c}
            5 -> do n <- int64; return columnmetadata{columnNumValues = n}
            6 -> do u <- int64; return columnmetadata{columnTotalUncompressedSize = u}
            7 -> do c <- int64; return columnmetadata{columnTotalCompressedSize = c}
            8 -> do
                kv <- thriftList keyValuePair
                return columnmetadata{columnKeyValueMetadata = kv}
            9 -> do o <- int64; return columnmetadata{columnDataPageOffset = o}
            10 -> do o <- int64; return columnmetadata{columnIndexPageOffset = o}
            11 -> do o <- int64; return columnmetadata{columnDictionaryPageOffset = o}
            12 -> do s <- statistics; return columnmetadata{columnStatistics = s}
            13 -> do
                pes <- thriftList pageEncodingStats
                return columnmetadata{columnEncodingStats = pes}
            14 -> do o <- int64; return columnmetadata{bloomFilterOffset = o}
            15 -> do l <- int32; return columnmetadata{bloomFilterLength = l}
            16 -> do ss <- sizeStatistics; return columnmetadata{columnSizeStatistics = ss}
            17 ->
                skipField compacttype
                    >> return columnmetadata{columnGeospatialStatistics = emptyGeospatialStatistics}
            _ -> skipField compacttype >> return columnmetadata

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L267
statistics :: ParquetReader ColumnStatistics
statistics =
    parseStruct emptyColumnStatistics $ \columnstatistics fieldid compacttype ->
        case fieldid of
            1 -> do b <- thriftBinary; return columnstatistics{columnMax = b}
            2 -> do b <- thriftBinary; return columnstatistics{columnMin = b}
            3 -> do n <- int64; return columnstatistics{columnNullCount = n}
            4 -> do d <- int64; return columnstatistics{columnDistictCount = d}
            5 -> do b <- thriftBinary; return columnstatistics{columnMaxValue = b}
            6 -> do b <- thriftBinary; return columnstatistics{columnMinValue = b}
            7 -> do b <- byte; return columnstatistics{isColumnMaxValueExact = b == 0x01}
            8 -> do b <- byte; return columnstatistics{isColumnMinValueExact = b == 0x01}
            _ -> skipField compacttype >> return columnstatistics

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L202
sizeStatistics :: ParquetReader SizeStatistics
sizeStatistics =
    parseStruct emptySizeStatistics $ \sizestatistics fieldid compacttype ->
        case fieldid of
            1 -> do u <- int64; return sizestatistics{unencodedByteArrayDataTypes = u}
            2 -> do h <- thriftList int64; return sizestatistics{repetitionLevelHistogram = h}
            3 -> do h <- thriftList int64; return sizestatistics{definitionLevelHistogram = h}
            _ -> skipField compacttype >> return sizestatistics

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L841
keyValuePair :: ParquetReader KeyValue
keyValuePair =
    parseStruct emptyKeyValue $ \keyvalue fieldid compacttype ->
        case fieldid of
            1 -> do k <- thriftString; return keyvalue{key = k}
            2 -> do v <- thriftString; return keyvalue{value = v}
            _ -> skipField compacttype >> return keyvalue

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L864
pageEncodingStats :: ParquetReader PageEncodingStats
pageEncodingStats =
    parseStruct emptyPageEncodingStats $ \pageencoding fieldid compacttype ->
        case fieldid of
            1 -> do t <- int32; return pageencoding{pageEncodingPageType = pageTypeFromInt t}
            2 -> do e <- int32; return pageencoding{pageEncoding = parquetEncodingFromInt e}
            3 -> do c <- int32; return pageencoding{pagesWithEncoding = c}
            _ -> skipField compacttype >> return pageencoding

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L849
sortingColumn :: ParquetReader SortingColumn
sortingColumn =
    parseStruct emptySortingColumn $ \sortingcolumn fieldid compacttype ->
        case fieldid of
            1 -> do i <- int32; return sortingcolumn{columnIndex = i}
            2 -> do b <- byte; return sortingcolumn{columnOrderDescending = b == 0x01}
            3 -> do b <- byte; return sortingcolumn{nullFirst = b == 0x01}
            _ -> skipField compacttype >> return sortingcolumn

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1065
columnOrder :: ParquetReader ColumnOrder
columnOrder =
    parseStruct COLUMN_ORDER_UNKNOWN $ \columnorder fieldid _ ->
        case fieldid of
            1 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return TYPE_ORDER
            _ -> return columnorder

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1269
parseEncryptionAlgorithm :: ParquetReader EncryptionAlgorithm
parseEncryptionAlgorithm =
    parseStruct ENCRYPTION_ALGORITHM_UNKNOWN $ \encryption fieldid compacttype ->
        case fieldid of
            1 -> aesGcmV1 (AesGcmV1 BS.empty BS.empty False)
            2 -> aesGcmCtrV1 (AesGcmCtrV1 BS.empty BS.empty False)
            _ -> skipField compacttype >> return encryption

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1245
aesGcmV1 :: EncryptionAlgorithm -> ParquetReader EncryptionAlgorithm
aesGcmV1 v =
    parseStruct v $ \encryption fieldid compacttype ->
        case encryption of
            AesGcmV1{} ->
                case fieldid of
                    1 -> do a <- thriftBinary; aesGcmV1 (encryption{aadPrefix = a})
                    2 -> do a <- thriftBinary; aesGcmV1 (encryption{aadFileUnique = a})
                    3 -> do b <- byte; aesGcmV1 (encryption{supplyAadPrefix = b == 0x01})
                    _ -> skipField compacttype >> return encryption
            _ -> return encryption

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1257
aesGcmCtrV1 :: EncryptionAlgorithm -> ParquetReader EncryptionAlgorithm
aesGcmCtrV1 v =
    parseStruct v $ \encryption fieldid compacttype ->
        case encryption of
            AesGcmCtrV1{} ->
                case fieldid of
                    1 -> do a <- thriftBinary; aesGcmCtrV1 (encryption{aadPrefix = a})
                    2 -> do a <- thriftBinary; aesGcmCtrV1 (encryption{aadFileUnique = a})
                    3 -> do b <- byte; aesGcmCtrV1 (encryption{supplyAadPrefix = b == 0x01})
                    _ -> skipField compacttype >> return encryption
            _ -> return encryption

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L471
parseLogicalType :: ParquetReader LogicalType
parseLogicalType =
    parseStruct LOGICAL_TYPE_UNKNOWN $ \logicaltype fieldid compacttype ->
        case fieldid of
            1 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return STRING_TYPE
            2 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return MAP_TYPE
            3 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return LIST_TYPE
            4 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return ENUM_TYPE
            5 -> decimalType 0 0
            6 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return DATE_TYPE
            7 -> timeType False MILLISECONDS
            8 -> timestampType False MILLISECONDS
            9 ->
                parseStruct () (\_ _ ct' -> void $ skipField ct') >> return LOGICAL_TYPE_UNKNOWN
            10 -> intType 0 False
            11 ->
                parseStruct () (\_ _ ct' -> void $ skipField ct') >> return LOGICAL_TYPE_UNKNOWN
            12 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return JSON_TYPE
            13 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return BSON_TYPE
            14 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return UUID_TYPE
            15 -> parseStruct () (\_ _ ct' -> void $ skipField ct') >> return FLOAT16_TYPE
            16 -> throwParseError "Variant fields are unsupported"
            17 -> throwParseError "Geometry fields are unsupported"
            18 -> throwParseError "Geography fields are unsupported"
            _ -> skipField compacttype >> return logicaltype

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L343
decimalType :: Int32 -> Int32 -> ParquetReader LogicalType
decimalType prec sc =
    parseStruct (DecimalType prec sc) $ \logicaltype fieldid compacttype ->
        case fieldid of
            1 -> do s <- int32; decimalType prec s
            2 -> do p <- int32; decimalType p sc
            _ -> skipField compacttype >> return logicaltype

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L373
timeType :: Bool -> TimeUnit -> ParquetReader LogicalType
timeType adj unit =
    parseStruct (TimeType adj unit) $ \logicaltype fieldid compacttype ->
        case fieldid of
            1 -> do b <- byte; timeType (b == 0x01) unit
            2 -> do u <- timeUnit; timeType adj u
            _ -> skipField compacttype >> return logicaltype

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L363
timestampType :: Bool -> TimeUnit -> ParquetReader LogicalType
timestampType adj unit =
    parseStruct (TimestampType adj unit) $ \logicaltype fieldid compacttype ->
        case fieldid of
            1 -> do _ <- byte; timestampType False unit
            2 -> do u <- timeUnit; timestampType adj u
            _ -> skipField compacttype >> return logicaltype

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L385
intType :: Int8 -> Bool -> ParquetReader LogicalType
intType bw signed =
    parseStruct (IntType bw signed) $ \logicaltype fieldid compacttype ->
        case fieldid of
            1 -> do b <- byte; intType (fromIntegral b) signed
            2 -> do b <- byte; intType bw (b == 0x01)
            _ -> skipField compacttype >> return logicaltype

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L352
timeUnit :: ParquetReader TimeUnit
timeUnit =
    parseStruct TIME_UNIT_UNKNOWN $ \timeunit fieldid compacttype ->
        case fieldid of
            1 -> return MILLISECONDS
            2 -> return MICROSECONDS
            3 -> return NANOSECONDS
            _ -> skipField compacttype >> return timeunit

parseFileMetadata ::
    (RandomAccess r) => r FileMetadata
parseFileMetadata = do
    footerOffset <- readSuffix 8
    let size = getMetadataSize footerOffset
    rawMetadata <- readSuffix (size + 8) <&> BS.take size
    case runParquetReader fileMetadata rawMetadata of
        Left e -> error $ show e
        Right (metadata, _) -> return metadata
  where
    getMetadataSize footer =
        let sizes :: [Int]
            sizes = map (fromIntegral . BS.index footer) [0 .. 3]
         in foldl' (.|.) 0 $ zipWith shiftL sizes [0, 8 .. 24]
