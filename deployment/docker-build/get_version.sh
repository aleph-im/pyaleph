# Returns the Docker image tag and its PEP440 equivalent to set as versions for
# the Docker image and the Python package, respectively.
function get_version() {
  IMAGE_TAG=$(git describe --tags --exact-match 2>/dev/null) || true
  PEP440_VERSION="${IMAGE_TAG}"
  if [ -z "${IMAGE_TAG}" ]; then
      latest_release=$(git describe --tags --abbrev=0 2>/dev/null || echo "0.0.0")
      commit_hash=$(git rev-parse --short HEAD)
      IMAGE_TAG="${latest_release}-${commit_hash}"
      # PEP440-compatible version
      PEP440_VERSION="${latest_release}+${commit_hash}"
  fi
}
