class Cloudcat < Formula
  desc "Preview and analyze data files in cloud storage from your terminal"
  homepage "https://github.com/jonathansudhakar1/cloudcat"
  version "0.2.6"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/jonathansudhakar1/cloudcat/releases/download/v0.2.6/cloudcat-0.2.6-macos-arm64.tar.gz"
      sha256 "8f1671529ab442b61efe1c901737a33c8e5a5d80d8e5ad10d16d37d96e7f55ad"
    end
    on_intel do
      odie "Intel Macs are not supported. Please use: pip install 'cloudcat[all]'"
    end
  end

  def install
    # Install all extracted files to libexec
    libexec.install Dir["*"]
    # Create a symlink in bin
    bin.install_symlink libexec/"cloudcat"
  end

  def caveats
    <<~EOS
      cloudcat requires cloud provider credentials to be configured:

      Google Cloud Storage:
        gcloud auth application-default login

      Amazon S3:
        aws configure

      Azure Blob Storage:
        az login
        # or set AZURE_STORAGE_CONNECTION_STRING

      For more information, see:
        https://github.com/jonathansudhakar1/cloudcat#authentication

      Note: On first run, you may need to allow the app in System Settings > Privacy & Security,
      or run: xattr -d com.apple.quarantine $(which cloudcat)
    EOS
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/cloudcat --help")
  end
end
