# Returns the Docker image tag and its PEP440 equivalent.
#
# When HEAD is exactly on a version tag (X.Y.Z, with optional pre-release
# suffix), use that tag verbatim. Otherwise, fall back to
# "<latest-release>-<short-hash>" for dev builds.
#
# Outputs (sourced by callers):
#   IMAGE           — Docker image name  (e.g. "alephim/pyaleph-node")
#   IMAGE_TAG       — Docker image tag   (e.g. "0.10.0", "0.10.1-rc0", "0.10.0-abc1234")
#   PEP440_VERSION  — Python-compatible   (e.g. "0.10.0", "0.10.1-rc0", "0.10.0+abc1234")

IMAGE="alephim/pyaleph-node"

function get_version() {
  # Try an exact version tag on HEAD. Match only tags that look like
  # version numbers. sort -rV picks the highest if multiple tags exist.
  IMAGE_TAG=$(git tag --points-at HEAD \
    | grep -E '^[0-9]+\.[0-9]+\.[0-9]+' || true \
    | sort -rV \
    | head -1)

  PEP440_VERSION="${IMAGE_TAG}"

  if [ -z "${IMAGE_TAG}" ]; then
    latest_release=$(git describe --tags --abbrev=0 --match '[0-9]*.[0-9]*.[0-9]*' 2>/dev/null \
      || echo "0.0.0")
    commit_hash=$(git rev-parse --short HEAD)
    IMAGE_TAG="${latest_release}-${commit_hash}"
    # PEP440 uses + instead of - for local versions
    PEP440_VERSION="${latest_release}+${commit_hash}"
  fi
}
