{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

module DataFrame.IO.Unstable.Parquet.Thrift.Compact (
    ParquetMetadataParser (..),
    ParquetMetadataException (..),
    throwParseError,
    byte,
    bytes,
    lookahead,
    varInt,
    toZigzag,
    fromZigzag,
    int32,
    int64,
    thriftBinary,
    thriftString,
    thriftDouble,
    CompactType (..),
    toCompactType,
    parseStruct,
    thriftList,
    skipField,
)
where

import Control.Monad (replicateM, replicateM_, void, when)
import Data.Bits (shiftL, shiftR, xor, (.&.), (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.ByteString.Unsafe (unsafeDrop, unsafeHead, unsafeTail, unsafeTake)
import Data.Char (chr)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import Data.Word (Word64, Word8)
import GHC.Float (castWord64ToDouble)

-- This module provides the combinators used to
-- parse the metadata of parquet files which are
-- encoded by the Thrift compact protocol
-- (https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
-- The metadata structure is described by https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift

data ParquetMetadataException
    = InputEmpty
    | InsufficientInput
    | NegativeBytes
    | ParseError String
    deriving (Eq, Show)

newtype ParquetMetadataParser a
    = ParquetMetadataParser
    { runParquetMetadataParser ::
        ByteString ->
        Either ParquetMetadataException (a, ByteString)
    }

instance Functor ParquetMetadataParser where
    fmap f (ParquetMetadataParser run) = ParquetMetadataParser $ fmap (first f) . run

instance Applicative ParquetMetadataParser where
    pure a = ParquetMetadataParser (\input -> Right (a, input))
    (ParquetMetadataParser p1) <*> (ParquetMetadataParser p2) = ParquetMetadataParser $ \input -> do
        (f, input') <- p1 input
        (a, input'') <- p2 input'
        return (f a, input'')

instance Monad ParquetMetadataParser where
    return = pure
    (ParquetMetadataParser run) >>= f = ParquetMetadataParser $ \input -> do
        (a, input') <- run input
        runParquetMetadataParser (f a) input'

first :: (a -> b) -> (a, c) -> (b, c)
first f (a, c) = (f a, c)

throwParseError :: String -> ParquetMetadataParser a
throwParseError msg = ParquetMetadataParser $ \_ -> Left (ParseError msg)

byte :: ParquetMetadataParser Word8
byte = ParquetMetadataParser p
  where
    -- internally just increments the bytestring pointer by one (and decrements the length)
    p input
        | ByteString.length input > 0 = Right (unsafeHead input, unsafeTail input)
        | otherwise = Left InputEmpty

bytes :: Int -> ParquetMetadataParser ByteString
bytes n = ParquetMetadataParser p
  where
    -- unsafeTake creates a bytestring with the same pointer but the length is different
    -- unsafeDrop simply adds n to the pointer and shortens the length by n
    p input
        | n < 0 = Left NegativeBytes
        | ByteString.length input < n = Left InsufficientInput
        | otherwise = Right (unsafeTake n input, unsafeDrop n input)

lookahead :: ParquetMetadataParser a -> ParquetMetadataParser a
lookahead (ParquetMetadataParser run) = ParquetMetadataParser $ \input -> do
    (a, _) <- run input
    return (a, input)

varInt :: ParquetMetadataParser Word64
varInt = ParquetMetadataParser $ go 0 0 0
  where
    go !result !shift !i input
        | i >= 10 = Right (result, input)
        | ByteString.null input = Left InsufficientInput
        | otherwise =
            let b = unsafeHead input
                rest = unsafeTail input
                payload = fromIntegral (b .&. 0x7F) :: Word64
                res = result .|. (payload `shiftL` shift)
             in if b < 0x80
                    then Right (res, rest)
                    else go res (shift + 7) (i + 1) rest

toZigzag :: Int64 -> Word64
toZigzag n = fromIntegral ((n `shiftL` 1) `xor` (n `shiftR` 63))

fromZigzag :: Word64 -> Int64
fromZigzag n = fromIntegral (n `shiftR` 1) `xor` negate (fromIntegral (n .&. 1))

int64 :: ParquetMetadataParser Int64
int64 = fromZigzag <$> varInt

int32 :: ParquetMetadataParser Int32
int32 = fromIntegral <$> int64

thriftBinary :: ParquetMetadataParser ByteString
thriftBinary = do
    len <- varInt
    let n = fromIntegral len
    if n < 0 then throwParseError "negative binary length" else bytes n

thriftString :: ParquetMetadataParser String
thriftString = map (chr . fromIntegral) . ByteString.unpack <$> thriftBinary

thriftDouble :: ParquetMetadataParser Double
thriftDouble = do
    bs <- bytes 8
    if ByteString.length bs < 8
        then throwParseError "insufficient bytes for double"
        else
            let w =
                    foldr (.|.) (0 :: Word64) $
                        zipWith
                            (\b sh -> fromIntegral b `shiftL` sh)
                            (ByteString.unpack bs)
                            [0, 8 .. 56]
             in return (castWord64ToDouble w)

data CompactType
    = CompactBoolTrue
    | CompactBoolFalse
    | CompactI8
    | CompactI16
    | CompactI32
    | CompactI64
    | CompactDouble
    | CompactBinary
    | CompactList
    | CompactSet
    | CompactMap
    | CompactStruct
    | CompactUUID
    deriving (Show, Eq, Enum, Bounded)

toCompactType :: Int -> Maybe CompactType
toCompactType 1 = Just CompactBoolTrue
toCompactType 2 = Just CompactBoolFalse
toCompactType 3 = Just CompactI8
toCompactType 4 = Just CompactI16
toCompactType 5 = Just CompactI32
toCompactType 6 = Just CompactI64
toCompactType 7 = Just CompactDouble
toCompactType 8 = Just CompactBinary
toCompactType 9 = Just CompactList
toCompactType 10 = Just CompactSet
toCompactType 11 = Just CompactMap
toCompactType 12 = Just CompactStruct
toCompactType 13 = Just CompactUUID
toCompactType _ = Nothing

splitByte :: Word8 -> (Int, Int)
splitByte word = (fromIntegral $ word `shiftR` 4, fromIntegral $ word .&. 0x0F)

fieldHeader :: Int -> ParquetMetadataParser (Maybe (Int, CompactType))
fieldHeader previousFieldId = do
    b <- byte
    if b == 0x00
        then return Nothing
        else do
            let (delta, typeIdRaw) = splitByte b
            fieldId <-
                if delta == 0
                    -- Compact protocol encodes field IDs as zigzag int16
                    then fromIntegral <$> int64
                    else return (delta + previousFieldId)
            if fieldId > 32767
                then throwParseError "fieldId in struct exceeded maximum value"
                else case toCompactType typeIdRaw of
                    Just ct -> return (Just (fieldId, ct))
                    Nothing -> return (Just (fieldId, CompactStruct))

parseStruct ::
    a ->
    (a -> Int -> CompactType -> ParquetMetadataParser a) ->
    ParquetMetadataParser a
parseStruct initial handleField = go 0 initial
  where
    go prevFieldId acc = do
        header <- fieldHeader prevFieldId
        case header of
            Nothing -> return acc
            Just (fieldId, ct) -> do
                acc' <- handleField acc fieldId ct
                go fieldId acc'

listHeader :: ParquetMetadataParser (Int, CompactType)
listHeader = do
    b <- byte
    let sizeNibble = fromIntegral ((b `shiftR` 4) .&. 0x0F)
        typeIdRaw = fromIntegral (b .&. 0x0F)
    size <-
        if sizeNibble == 15
            then fromIntegral <$> varInt
            else return sizeNibble
    case toCompactType typeIdRaw of
        Just ct -> return (size, ct)
        Nothing -> return (size, CompactStruct)

thriftList :: ParquetMetadataParser a -> ParquetMetadataParser [a]
thriftList elementParser = do
    (size, _) <- listHeader
    if size <= 0 then return [] else replicateM size elementParser

skipField :: CompactType -> ParquetMetadataParser ()
skipField = \case
    CompactBoolTrue -> return ()
    CompactBoolFalse -> return ()
    CompactI8 -> void byte
    CompactI16 -> void int64
    CompactI32 -> void int32
    CompactI64 -> void int64
    CompactDouble -> void thriftDouble
    CompactBinary -> void thriftBinary
    CompactList -> do
        (n, elemCt) <- listHeader
        replicateM_ n (skipField elemCt)
    CompactSet -> do
        (n, elemCt) <- listHeader
        replicateM_ n (skipField elemCt)
    CompactMap -> do
        n <- varInt
        when (n > 0) $ do
            b <- byte
            let keyN = fromIntegral (b `shiftR` 4) .&. 0x0F
                valN = fromIntegral (b .&. 0x0F)
            let keyCt = fromMaybe CompactStruct (toCompactType keyN)
                valCt = fromMaybe CompactStruct (toCompactType valN)
            replicateM_ (fromIntegral n) (skipField keyCt >> skipField valCt)
    CompactStruct -> skipToStructEnd
    CompactUUID -> void (bytes 16)

skipToStructEnd :: ParquetMetadataParser ()
skipToStructEnd = do
    header <- fieldHeader 0
    case header of
        Nothing -> return ()
        Just (_, ct) -> do
            skipField ct
            skipToStructEnd
