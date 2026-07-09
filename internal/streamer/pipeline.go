package streamer

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

const (
	SampleRate = "32000"
	Channels   = "1"
	Format     = "s16le"
	Codec      = "pcm_s16le"
)

// BytesPerSec = 32000 * 2 * 1 = 64000  (used for seek byte→time conversion)
const BytesPerSec = 64_000

// findYtDlp looks for the yt-dlp binary.
func findYtDlp() string {
	if v := os.Getenv("YTDLP_BIN"); v != "" {
		return v
	}
	exe, _ := os.Executable()
	candidate := filepath.Join(filepath.Dir(exe), "bin", "yt-dlp")
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}
	if p, err := exec.LookPath("yt-dlp"); err == nil {
		return p
	}
	return "yt-dlp"
}

// BuildFFmpegFromFile returns an exec.Cmd that reads a local file
// and outputs raw PCM to stdout.
// seekSeconds > 0 inserts -ss <seekSeconds> before -i for pause/resume.
func BuildFFmpegFromFile(ctx context.Context, filePath string, seekSeconds float64) *exec.Cmd {
	var args []string
	if seekSeconds > 0 {
		seekStr := strconv.FormatFloat(seekSeconds, 'f', -1, 64)
		args = append(args, "-ss", seekStr)
	}
	args = append(args,
		"-i", filePath,
		"-f", Format,
		"-acodec", Codec,
		"-ar", SampleRate,
		"-ac", Channels,
		"-",
	)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return cmd.Process.Signal(os.Kill)
		}
		return nil
	}
	return cmd
}

// BuildYtDlpStream returns an exec.Cmd that writes audio bytes to its stdout.
// The caller must pipe YtDlp.Stdout → FFmpeg.Stdin.
func BuildYtDlpStream(ctx context.Context, videoID string) *exec.Cmd {
	ytdlpBin := findYtDlp()
	args := []string{
		"--js-runtimes", "node",
		"-f", "bestaudio/best",
		"-o", "-",
		"--",
		videoID,
	}
	cmd := exec.CommandContext(ctx, ytdlpBin, args...)
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return cmd.Process.Signal(os.Kill)
		}
		return nil
	}
	return cmd
}

// BuildFFmpegFromStdin returns an exec.Cmd reading from stdin (connected to yt-dlp).
func BuildFFmpegFromStdin(ctx context.Context) *exec.Cmd {
	args := []string{
		"-i", "-",
		"-f", Format,
		"-acodec", Codec,
		"-ar", SampleRate,
		"-ac", Channels,
		"-",
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return cmd.Process.Signal(os.Kill)
		}
		return nil
	}
	return cmd
}
