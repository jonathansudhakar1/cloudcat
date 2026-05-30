"""Progress indicator for CLI operations."""

import sys
import threading
import time
from typing import Optional
from colorama import Fore, Style


class ProgressIndicator:
    """A simple animated progress indicator that updates in place.

    Shows a spinner animation with a status message that updates on a single line.
    """

    SPINNER_FRAMES = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

    def __init__(self):
        self._message: str = ""
        self._running: bool = False
        self._enabled: bool = False
        self._thread: Optional[threading.Thread] = None
        self._frame_index: int = 0
        self._lock = threading.Lock()

    def start(self, message: str = "Working...") -> None:
        """Start the progress indicator with an initial message.

        The spinner is written to stderr and only when stderr is an interactive
        terminal, so it never corrupts piped/redirected data on stdout.
        """
        # Only animate for interactive terminals; stay silent when redirected.
        if not sys.stderr.isatty():
            self._enabled = False
            return

        self._enabled = True
        with self._lock:
            self._message = message
            self._running = True
            self._frame_index = 0

        self._thread = threading.Thread(target=self._animate, daemon=True)
        self._thread.start()

    def update(self, message: str) -> None:
        """Update the progress message."""
        if not self._enabled:
            return
        with self._lock:
            self._message = message

    def stop(self, final_message: Optional[str] = None) -> None:
        """Stop the progress indicator and optionally show a final message."""
        with self._lock:
            self._running = False

        if self._thread:
            self._thread.join(timeout=0.5)
            self._thread = None

        # Clear the current line on stderr (where the spinner was drawn).
        if self._enabled:
            sys.stderr.write('\r' + ' ' * 120 + '\r')
            sys.stderr.flush()

        # Show final message if provided
        if final_message:
            sys.stderr.write(final_message + '\n')
            sys.stderr.flush()

    def _animate(self) -> None:
        """Animation loop running in a separate thread."""
        while True:
            with self._lock:
                if not self._running:
                    break
                message = self._message
                frame = self.SPINNER_FRAMES[self._frame_index]
                self._frame_index = (self._frame_index + 1) % len(self.SPINNER_FRAMES)

            # Write spinner and message to stderr, overwriting the line.
            output = f'\r{Fore.CYAN}{frame}{Style.RESET_ALL} {message}'
            # Pad to clear any leftover characters from longer previous messages
            output = output.ljust(120)
            sys.stderr.write(output)
            sys.stderr.flush()

            time.sleep(0.08)


# Global instance for easy access
_progress: Optional[ProgressIndicator] = None


def start_progress(message: str = "Working...") -> None:
    """Start a global progress indicator."""
    global _progress
    if _progress is None:
        _progress = ProgressIndicator()
    _progress.start(message)


def update_progress(message: str) -> None:
    """Update the global progress indicator message."""
    global _progress
    if _progress:
        _progress.update(message)


def stop_progress(final_message: Optional[str] = None) -> None:
    """Stop the global progress indicator."""
    global _progress
    if _progress:
        _progress.stop(final_message)
        _progress = None
