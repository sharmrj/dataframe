{-# LANGUAGE LambdaCase #-}

module DataFrame.IO.Unstable.Parquet.Thrift.Compact (
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
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.Char (chr)
import Data.Maybe (fromMaybe)
import Data.Word (Word64, Word8)
import GHC.Float (castWord64ToDouble)

import DataFrame.IO.Unstable.Parquet.Reader (
    ParquetReader (..),
    byte,
    bytes,
    int32,
    int64,
    throwParseError,
    varInt,
 )

-- This module provides the combinators used to
-- parse the metadata of parquet files which are
-- encoded by the Thrift compact protocol
-- (https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
-- The metadata structure is described by https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift

thriftBinary :: ParquetReader ByteString
thriftBinary = do
    len <- varInt
    let n = fromIntegral len
    if n < 0 then throwParseError "negative binary length" else bytes n

thriftString :: ParquetReader String
thriftString = map (chr . fromIntegral) . ByteString.unpack <$> thriftBinary

thriftDouble :: ParquetReader Double
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

fieldHeader :: Int -> ParquetReader (Maybe (Int, CompactType))
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
    (a -> Int -> CompactType -> ParquetReader a) ->
    ParquetReader a
parseStruct initial handleField = go 0 initial
  where
    go prevFieldId acc = do
        header <- fieldHeader prevFieldId
        case header of
            Nothing -> return acc
            Just (fieldId, ct) -> do
                acc' <- handleField acc fieldId ct
                go fieldId acc'

listHeader :: ParquetReader (Int, CompactType)
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

thriftList :: ParquetReader a -> ParquetReader [a]
thriftList elementParser = do
    (size, _) <- listHeader
    if size <= 0 then return [] else replicateM size elementParser

skipField :: CompactType -> ParquetReader ()
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

skipToStructEnd :: ParquetReader ()
skipToStructEnd = do
    header <- fieldHeader 0
    case header of
        Nothing -> return ()
        Just (_, ct) -> do
            skipField ct
            skipToStructEnd
