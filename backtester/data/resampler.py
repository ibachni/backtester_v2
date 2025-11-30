from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Optional

from backtester.types.aliases import UnixMillis
from backtester.types.types import Candle


@dataclass(slots=True)
class ResampleIter:
    """
    Wrap a lower-tf candle AsyncIterator (e.g., "1 min") and emit higher timeframe aggregated
    candles (e.g., 5 min); BarFeed is not touched.
    """

    it: AsyncIterator[Candle]
    symbol: str
    timeframe_out: str
    tf_ms_out: UnixMillis
    _carry: Optional[Candle] = None
    _done: bool = False

    def __aiter__(self) -> AsyncIterator[Candle]:
        return self

    async def __anext__(self) -> Candle:
        if self._done:
            raise StopAsyncIteration

        # Seed the bucket with the first candle (from carry or upstream)
        c = self._carry if self._carry is not None else await anext(self.it)
        self._carry = None

        # Calculate the starting point of the bucket
        bucket_start = (c.start_ms // self.tf_ms_out) * self.tf_ms_out
        # Calculate the end point of the bucket
        bucket_end = bucket_start + self.tf_ms_out

        o = c.open
        h = c.high
        low_ = c.low
        v = c.volume
        trades = c.trades or 0
        last_close = c.close

        all_final = True if c.is_final is None else bool(c.is_final)

        while True:
            try:
                n = await anext(self.it)
            except StopAsyncIteration:
                self._done = True
                return Candle(
                    symbol=self.symbol,
                    timeframe=self.timeframe_out,
                    start_ms=bucket_start,
                    end_ms=bucket_end,
                    open=o,
                    high=h,
                    low=low_,
                    close=last_close,
                    volume=v,
                    trades=trades,
                    is_final=all_final,
                )

            if n.start_ms < bucket_end:
                # Same bucket
                h = max(h, n.high)
                low_ = min(low_, n.low)
                v += n.volume
                trades += n.trades or 0
                last_close = n.close
                if n.is_final is False:
                    all_final = False
            else:
                # Next bucket begins with n
                self._carry = n
                return Candle(
                    symbol=self.symbol,
                    timeframe=self.timeframe_out,
                    start_ms=bucket_start,
                    end_ms=bucket_end,
                    open=o,
                    high=h,
                    low=low_,
                    close=last_close,
                    volume=v,
                    trades=trades,
                    is_final=all_final,
                )
