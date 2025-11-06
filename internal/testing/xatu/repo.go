// Package xatu provides Xatu repository management and migration handling.
package xatu

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

const (
	defaultXatuRepoURL = "https://github.com/ethpandaops/xatu"
	defaultXatuRef     = "master"
	xatuDirName        = "xatu"
)

// RepoManager manages the xatu repository for migrations.
type RepoManager struct {
	workDir string
	repoURL string
	ref     string
	log     logrus.FieldLogger
}

// NewRepoManager creates a new xatu repository manager.
func NewRepoManager(log logrus.FieldLogger, workDir, repoURL, ref string) *RepoManager {
	if repoURL == "" {
		repoURL = defaultXatuRepoURL
	}

	if ref == "" {
		ref = defaultXatuRef
	}

	return &RepoManager{
		workDir: workDir,
		repoURL: repoURL,
		ref:     ref,
		log:     log.WithField("component", "xatu_repo"),
	}
}

// GetMigrationDir returns the path to xatu's migration directory.
func (r *RepoManager) GetMigrationDir(repoPath string) string {
	return filepath.Join(repoPath, "deploy", "migrations", "clickhouse")
}

// EnsureRepo ensures the xatu repository is cloned and at the correct ref.
func (r *RepoManager) EnsureRepo() (string, error) {
	repoPath := filepath.Join(r.workDir, xatuDirName)

	if _, err := os.Stat(filepath.Join(repoPath, ".git")); err == nil {
		r.log.WithField("path", repoPath).Debug("xatu repository already exists")

		if err := r.gitFetch(repoPath); err != nil {
			r.log.WithError(err).Warn("failed to fetch latest changes, continuing with existing repo")
		}

		if err := r.gitCheckout(repoPath); err != nil {
			return "", fmt.Errorf("checking out ref %s: %w", r.ref, err)
		}

		return repoPath, nil
	}

	r.log.WithFields(logrus.Fields{
		"url": r.repoURL,
		"ref": r.ref,
		"dir": repoPath,
	}).Info("cloning xatu repository")

	if err := r.gitClone(repoPath); err != nil {
		return "", fmt.Errorf("cloning repository: %w", err)
	}

	if err := r.gitCheckout(repoPath); err != nil {
		return "", fmt.Errorf("checking out ref %s: %w", r.ref, err)
	}

	r.log.Info("xatu repository ready")

	return repoPath, nil
}

// gitClone clones the repository.
func (r *RepoManager) gitClone(dest string) error {
	cmd := exec.Command("git", "clone", "--depth", "1", "--branch", r.ref, r.repoURL, dest) //nolint:gosec // G204: Git command with controlled repository URL and ref

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "%s", stderr.String())
		}

		if stdout.Len() > 0 {
			_, _ = fmt.Fprintf(os.Stdout, "%s", stdout.String())
		}

		return fmt.Errorf("git clone failed: %w", err)
	}

	return nil
}

// gitFetch fetches latest changes (shallow fetch of specific ref).
func (r *RepoManager) gitFetch(repoPath string) error {
	cmd := exec.Command("git", "fetch", "origin", "--depth", "1", r.ref) //nolint:gosec // G204: Git command with controlled ref
	cmd.Dir = repoPath

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "%s", stderr.String())
		}

		if stdout.Len() > 0 {
			_, _ = fmt.Fprintf(os.Stdout, "%s", stdout.String())
		}

		return fmt.Errorf("git fetch failed: %w", err)
	}

	return nil
}

// gitCheckout checks out the desired ref.
func (r *RepoManager) gitCheckout(repoPath string) error {
	cmd := exec.Command("git", "checkout", r.ref) //nolint:gosec // G204: Git command with controlled ref
	cmd.Dir = repoPath

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "%s", stderr.String())
		}

		if stdout.Len() > 0 {
			_, _ = fmt.Fprintf(os.Stdout, "%s", stdout.String())
		}

		return fmt.Errorf("git checkout failed: %w", err)
	}

	return nil
}
