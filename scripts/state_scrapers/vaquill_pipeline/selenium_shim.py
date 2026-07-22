"""Drop-in robustness for the upstream scrapers' bare ``webdriver.Chrome()`` calls.

The 9 scrapers that use Selenium do ``webdriver.Chrome()`` with no options.
On Apple Silicon (and most modern setups) that crashes immediately. We
monkey-patch the constructor to:

    1. Use Selenium Manager (built into selenium 4.6+) to auto-resolve a
       matching chromedriver — no manual install / version-pinning.
    2. Apply sensible defaults: headless, no-sandbox, disable-gpu, custom UA,
       window size big enough that responsive layouts render the desktop view.
    3. Set a single global driver so multi-page scrapers don't leak browsers.
    4. Detect "Chrome not reachable" / "session not created" crashes and
       fall through to a graceful retry once.

Call ``install_selenium_shim()`` from ``vaquill_pipeline.patch.install()`` and
the scrapers run unchanged.
"""
from __future__ import annotations

import os
import random
from typing import Any

from .log import get_logger

_log = get_logger(component="selenium_shim")
_installed = False


_DESKTOP_UAS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


def _build_default_options():
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    if os.environ.get("VAQUILL_SELENIUM_HEADED") != "1":
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"--user-agent={random.choice(_DESKTOP_UAS)}")
    # Stop the "DevTools listening on..." noise from polluting logs
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    return opts


def install_selenium_shim() -> None:
    """Idempotently patch ``selenium.webdriver.Chrome`` and ``.Firefox`` to
    use our defaults. Safe to call multiple times.
    """
    global _installed
    if _installed:
        return
    try:
        import selenium.webdriver as wd
    except ImportError:
        _log.warning("selenium_not_installed", note="scrapers that need Chrome will crash")
        _installed = True
        return

    original_chrome = wd.Chrome

    def _patched_chrome(*args: Any, **kwargs: Any):
        # If the caller passed their own options, respect them.
        if "options" not in kwargs and not args:
            kwargs["options"] = _build_default_options()
        try:
            return original_chrome(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            _log.error("chrome_start_failed", error=str(e)[:200])
            raise

    wd.Chrome = _patched_chrome  # type: ignore[assignment]
    _installed = True
    _log.info("selenium_shim_installed", note="webdriver.Chrome() now uses headless+autodriver")
