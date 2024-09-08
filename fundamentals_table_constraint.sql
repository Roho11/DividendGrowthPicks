ALTER TABLE fundamentals
ADD CONSTRAINT unique_ticker_lastUpdated UNIQUE (ticker, lastUpdated);

