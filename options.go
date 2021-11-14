package bitcask

type Option func(config *Config) error

func WithConfig(src *Config) Option {
	return func(config *Config) error {
		config.MaxFileSize = src.MaxFileSize
		config.MaxKeySize = src.MaxKeySize
		config.MaxValueSize = src.MaxValueSize
		config.Sync = src.Sync
		return nil
	}
}

func WithMaxFileSize(size int64) Option {
	return func(config *Config) error {
		config.MaxFileSize = size
		return nil
	}
}
