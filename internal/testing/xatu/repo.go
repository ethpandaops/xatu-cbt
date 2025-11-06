package xatu

import (
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

// RepoManager manages the xatu repository for migrations
type RepoManager struct {
	workDir string
	repoURL string
	ref     string
	log     logrus.FieldLogger
}

// NewRepoManager creates a new xatu repository manager
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

// EnsureRepo ensures the xatu repository is cloned and at the correct ref
func (r *RepoManager) EnsureRepo() (string, error) {
	repoPath := filepath.Join(r.workDir, xatuDirName)

	// Check if repo already exists
	if _, err := os.Stat(filepath.Join(repoPath, ".git")); err == nil {
		r.log.WithField("path", repoPath).Debug("xatu repository already exists")

		// Fetch latest changes
		if err := r.gitFetch(repoPath); err != nil {
			r.log.WithError(err).Warn("failed to fetch latest changes, continuing with existing repo")
		}

		// Checkout the desired ref
		if err := r.gitCheckout(repoPath); err != nil {
			return "", fmt.Errorf("checking out ref %s: %w", r.ref, err)
		}

		return repoPath, nil
	}

	// Clone the repository
	r.log.WithFields(logrus.Fields{
		"url": r.repoURL,
		"ref": r.ref,
		"dir": repoPath,
	}).Info("cloning xatu repository")

	if err := r.gitClone(repoPath); err != nil {
		return "", fmt.Errorf("cloning repository: %w", err)
	}

	// Checkout the desired ref
	if err := r.gitCheckout(repoPath); err != nil {
		return "", fmt.Errorf("checking out ref %s: %w", r.ref, err)
	}

	r.log.Info("xatu repository ready")

	return repoPath, nil
}

// GetMigrationDir returns the path to xatu's migration directory
func (r *RepoManager) GetMigrationDir(repoPath string) string {
	return filepath.Join(repoPath, "deploy", "migrations", "clickhouse")
}

// gitClone clones the repository
func (r *RepoManager) gitClone(dest string) error {
	cmd := exec.Command("git", "clone", "--depth", "1", "--branch", r.ref, r.repoURL, dest)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git clone failed: %w", err)
	}

	return nil
}

// gitFetch fetches latest changes
func (r *RepoManager) gitFetch(repoPath string) error {
	cmd := exec.Command("git", "fetch", "origin")
	cmd.Dir = repoPath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git fetch failed: %w", err)
	}

	return nil
}

// gitCheckout checks out the desired ref
func (r *RepoManager) gitCheckout(repoPath string) error {
	cmd := exec.Command("git", "checkout", r.ref)
	cmd.Dir = repoPath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git checkout failed: %w", err)
	}

	return nil
}
