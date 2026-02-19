{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE BangPatterns #-}

module DataFrame.IO.Unstable.Parquet.Reader (
    ParquetReader (..),
    ParquetReaderException (..),
    throwParseError,
    byte,
    bytes,
    lookahead,
    varInt,
    toZigzag,
    fromZigzag,
    int32,
    int64,
)
where

import Data.Bits (shiftL, shiftR, xor, (.&.), (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.ByteString.Unsafe (unsafeDrop, unsafeHead, unsafeTail, unsafeTake)
import Data.Int (Int32, Int64)
import Data.Word (Word64, Word8)

data ParquetReaderException
    = InputEmpty
    | InsufficientInput
    | NegativeBytes
    | ParseError String
    deriving (Eq, Show)

newtype ParquetReader a
    = ParquetReader
    { runParquetReader ::
        ByteString ->
        Either ParquetReaderException (a, ByteString)
    }

instance Functor ParquetReader where
    fmap f (ParquetReader run) = ParquetReader $ fmap (first f) . run

instance Applicative ParquetReader where
    pure a = ParquetReader (\input -> Right (a, input))
    (ParquetReader p1) <*> (ParquetReader p2) = ParquetReader $ \input -> do
        (f, input') <- p1 input
        (a, input'') <- p2 input'
        return (f a, input'')

instance Monad ParquetReader where
    return = pure
    (ParquetReader run) >>= f = ParquetReader $ \input -> do
        (a, input') <- run input
        runParquetReader (f a) input'

first :: (a -> b) -> (a, c) -> (b, c)
first f (a, c) = (f a, c)

throwParseError :: String -> ParquetReader a
throwParseError msg = ParquetReader $ \_ -> Left (ParseError msg)

byte :: ParquetReader Word8
byte = ParquetReader p
  where
    p input
        | ByteString.length input > 0 = Right (unsafeHead input, unsafeTail input)
        | otherwise = Left InputEmpty

bytes :: Int -> ParquetReader ByteString
bytes n = ParquetReader p
  where
    p input
        | n < 0 = Left NegativeBytes
        | ByteString.length input < n = Left InsufficientInput
        | otherwise = Right (unsafeTake n input, unsafeDrop n input)

lookahead :: ParquetReader a -> ParquetReader a
lookahead (ParquetReader run) = ParquetReader $ \input -> do
    (a, _) <- run input
    return (a, input)

varInt :: ParquetReader Word64
varInt = ParquetReader $ go 0 0 0
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

int64 :: ParquetReader Int64
int64 = fromZigzag <$> varInt

int32 :: ParquetReader Int32
int32 = fromIntegral <$> int64
