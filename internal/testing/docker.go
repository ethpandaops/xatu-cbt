package testing

import "github.com/ethpandaops/xatu-cbt/internal/config"

func redisComposeArgs(redisCLIArgs ...string) []string {
	args := []string{
		"compose",
		"-f", config.PlatformComposeFile,
		"-p", config.GetProjectName(),
		"exec",
		"-T",
		config.RedisContainerName,
		"redis-cli",
	}

	return append(args, redisCLIArgs...)
}
