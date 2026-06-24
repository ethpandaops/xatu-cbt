package infra

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePublishedPort(t *testing.T) {
	published, err := parsePublishedPort([]byte("127.0.0.1:9100\n"))

	require.NoError(t, err)
	require.Equal(t, PublishedPort{Host: "127.0.0.1", Port: "9100"}, published)
}

func TestParsePublishedPortNormalizesWildcardHost(t *testing.T) {
	published, err := parsePublishedPort([]byte("0.0.0.0:9100\n"))

	require.NoError(t, err)
	require.Equal(t, PublishedPort{Host: "localhost", Port: "9100"}, published)
}
