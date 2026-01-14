// Package testing provides Xatu repository management and migration handling.
package testing

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
	shallow bool // Use shallow clone (--depth 1) for faster cloning
	log     logrus.FieldLogger
}

// NewRepoManager creates a new xatu repository manager.
// Uses shallow clones (--depth 1) only when using the default ref for faster cloning.
// Custom refs get full clones to allow easier branch switching.
func NewRepoManager(log logrus.FieldLogger, workDir, repoURL, ref string) *RepoManager {
	if repoURL == "" {
		repoURL = defaultXatuRepoURL
	}

	// Use shallow clone only for default ref (faster for typical usage)
	shallow := ref == "" || ref == defaultXatuRef

	if ref == "" {
		ref = defaultXatuRef
	}

	return &RepoManager{
		workDir: workDir,
		repoURL: repoURL,
		ref:     ref,
		shallow: shallow,
		log:     log.WithField("component", "xatu_repo"),
	}
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

	r.log.Info("xatu repository ready")

	return repoPath, nil
}

// gitClone clones the repository.
func (r *RepoManager) gitClone(dest string) error {
	args := []string{"clone"}
	if r.shallow {
		args = append(args, "--depth", "1")
	}
	args = append(args, "--branch", r.ref, r.repoURL, dest)

	cmd := exec.Command("git", args...) //nolint:gosec // G204: Git command with controlled repository URL and ref

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

// gitFetch fetches latest changes for the specific ref.
func (r *RepoManager) gitFetch(repoPath string) error {
	args := []string{"fetch", "origin"}
	if r.shallow {
		args = append(args, "--depth", "1")
	}
	args = append(args, r.ref)

	cmd := exec.Command("git", args...) //nolint:gosec // G204: Git command with controlled ref
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
// Uses -B to create/reset the branch to FETCH_HEAD (what was just fetched).
// Uses -f to force checkout, overwriting any conflicting untracked files.
func (r *RepoManager) gitCheckout(repoPath string) error {
	cmd := exec.Command("git", "checkout", "-f", "-B", r.ref, "FETCH_HEAD") //nolint:gosec // G204: Git command with controlled ref
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
