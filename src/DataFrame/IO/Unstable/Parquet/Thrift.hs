{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE BangPatterns #-}

module DataFrame.IO.Unstable.Parquet.Thrift where

import Data.Bits (shiftR, (.&.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.ByteString.Unsafe (unsafeDrop, unsafeHead, unsafeTail, unsafeTake)
import Data.Word (Word8)
import DataFrame.IO.Parquet.Thrift (FileMetadata)

-- This module parses the metadata of parquet files which are
-- encoded by the Thrift compact protocol
-- (https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
-- The metadata structure is described by https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift

data ParquetMetadataException
    = InputEmpty
    | InsufficientInput
    | NegativeBytes
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

struct :: ParquetMetadataParser FileMetadata
struct = undefined

stop :: ParquetMetadataParser Bool
stop = do
    b <- byte
    return $ b == 0x00

splitByte :: Word8 -> (Int, Int)
splitByte word = (fromIntegral $ word `shiftR` 4, fromIntegral $ word .&. 0x0F)

first :: (a -> b) -> (a, c) -> (b, c)
first f (a, c) = (f a, c)

lookahead :: ParquetMetadataParser a -> ParquetMetadataParser a
lookahead (ParquetMetadataParser run) = ParquetMetadataParser $ \input -> do
    (a, _) <- run input
    return (a, input)
