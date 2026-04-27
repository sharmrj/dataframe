{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}

module DataFrame.IO.Utils.RandomAccess where

import Control.Monad.IO.Class (MonadIO (..))
import Data.ByteString (ByteString)
import Data.ByteString.Internal (ByteString (PS))
import qualified Data.Vector.Storable as Storable
import Data.Word (Word8)
import DataFrame.IO.Parquet.Seeking (
    FileBufferedOrSeekable,
    fGet,
    fSeek,
    readLastBytes, fGetBuf,
 )
import DataFrame.IO.Parquet.Memory (Buffer(..))
import Foreign (castForeignPtr, Pool, pooledReallocBytes)
import System.IO (
    SeekMode (AbsoluteSeek),
 )

uncurry3 :: (a -> b -> c -> d) -> (a, b, c) -> d
uncurry3 f (a, b, c) = f a b c

data Range = Range {offset :: !Integer, length :: !Int} deriving (Eq, Show)

class (Monad m) => RandomAccess m where
    readBytes :: Range -> m ByteString
    readRanges :: [Range] -> m [ByteString]
    readRanges = mapM readBytes
    readSuffix :: Int -> m ByteString
    readBytesIntoBuffer :: Pool -> Buffer Word8 -> Range -> m (Buffer Word8)

newtype ReaderIO r a = ReaderIO {runReaderIO :: r -> IO a}

instance Functor (ReaderIO r) where
    fmap f (ReaderIO run) = ReaderIO $ fmap f . run

instance Applicative (ReaderIO r) where
    pure a = ReaderIO $ \_ -> pure a
    (ReaderIO fg) <*> (ReaderIO fa) = ReaderIO $ \r -> do
        a <- fa r
        g <- fg r
        pure (g a)

instance Monad (ReaderIO r) where
    return = pure
    (ReaderIO ma) >>= f = ReaderIO $ \r -> do
        a <- ma r
        runReaderIO (f a) r

instance MonadIO (ReaderIO r) where
    liftIO io = ReaderIO $ const io

type LocalFile = ReaderIO FileBufferedOrSeekable

instance RandomAccess LocalFile where
    readBytes (Range offset' length') = ReaderIO $ \handle -> do
        fSeek handle AbsoluteSeek offset'
        fGet handle length'
    readSuffix n = ReaderIO (readLastBytes $ fromIntegral n)
    readBytesIntoBuffer pool (Buffer bufPtr _ bufSize) (Range rangeOffset rangeLength)
      | rangeLength > bufSize = ReaderIO $ \handle -> do
        let bufSize' = rangeLength + 1024
        bufPtr' <- pooledReallocBytes pool bufPtr bufSize'
        fSeek handle AbsoluteSeek rangeOffset
        bufSize'' <- fGetBuf handle bufPtr' rangeLength
        return $ Buffer bufPtr' 0 bufSize''
      | otherwise = ReaderIO $ \handle -> do
        fSeek handle AbsoluteSeek rangeOffset
        bufSize' <- fGetBuf handle bufPtr rangeLength
        return $ Buffer bufPtr 0 bufSize'

type MMappedFile = ReaderIO (Storable.Vector Word8)

-- The instance exists but we don't have the means to mmap the file currently
instance RandomAccess MMappedFile where
    readBytes (Range offset' length') =
        ReaderIO $
            pure . unsafeToByteString . Storable.slice (fromInteger offset') length'
    readSuffix n =
        ReaderIO $ \v ->
            let len = Storable.length v
                n' = min n len
                start = len - n'
             in pure . unsafeToByteString $ Storable.slice start n' v
    -- Instead of mmapping thbe file upfront, we mmap the range itself.
    -- RollSafeThinkAboutIt.jpg
    readBytesIntoBuffer _ _ _ = undefined

unsafeToByteString :: Storable.Vector Word8 -> ByteString
unsafeToByteString v = PS (castForeignPtr ptr) offset' len
  where
    (ptr, offset', len) = Storable.unsafeToForeignPtr v
