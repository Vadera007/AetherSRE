"""
AetherSRE — Subprocess Remediation Executor
===========================================
Securely triggers localized terminal subprocesses for auto-execution runs.
Includes shell injection protections and execution timeouts.
"""

from __future__ import annotations

import asyncio
import shlex
from typing import Final

from app.core.logging_config import get_logger

logger = get_logger(__name__)

_EXECUTION_TIMEOUT_S: Final[float] = 5.0


class ExecutionResult(object):
    """Encapsulates subprocess run outputs."""

    def __init__(self, returncode: int, stdout: str, stderr: str, duration_s: float) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.duration_s = duration_s

    @property
    def is_success(self) -> bool:
        return self.returncode == 0


class LocalActionExecutor:
    """Handles secure asyncio subprocess executions with strict guards."""

    @staticmethod
    async def execute(command: str) -> ExecutionResult:
        """
        Executes a localized script/command. Splits input via shlex to avoid
        arbitrary shell execution vulnerabilities.
        """
        logger.info("LocalActionExecutor: Preparing to execute | command=%r", command)

        # shlex split prevents shell injections by passing arguments as an array
        args = shlex.split(command)
        if not args:
            return ExecutionResult(-1, "", "Empty command", 0.0)

        # For security, we map the command prefix to a mock script path, or execute safe echo stubs
        # In a real environment, we'd execute /app/scripts/remediation-runbook.sh
        # We will simulate execution by calling a safe target command or echo stubs.
        # If the script is mock-remediation, we run echo stubs.
        if args[0] == "mock-remediation":
            action = args[1] if len(args) > 1 else "benign"
            real_cmd = ["echo", f"[MOCK_HEALING] Executed action: {action}"]
        else:
            real_cmd = ["echo", f"Custom command: {shlex.join(args)}"]

        t0 = asyncio.get_running_loop().time()
        try:
            process = await asyncio.create_subprocess_exec(
                real_cmd[0],
                *real_cmd[1:],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Enforce execution timeout limit
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(),
                timeout=_EXECUTION_TIMEOUT_S,
            )

            duration = asyncio.get_running_loop().time() - t0
            stdout = stdout_bytes.decode("utf-8", errors="ignore").strip()
            stderr = stderr_bytes.decode("utf-8", errors="ignore").strip()
            returncode = process.returncode if process.returncode is not None else -1

            logger.info(
                "LocalActionExecutor: Command finished | returncode=%d duration=%.3fs stdout=%r",
                returncode,
                duration,
                stdout,
            )

            return ExecutionResult(returncode, stdout, stderr, duration)

        except asyncio.TimeoutError:
            duration = asyncio.get_running_loop().time() - t0
            logger.error("LocalActionExecutor: Timeout exceeded (%d seconds) | cmd=%s", _EXECUTION_TIMEOUT_S, command)
            return ExecutionResult(-2, "", f"Execution timed out after {_EXECUTION_TIMEOUT_S}s", duration)
        except Exception as exc:
            duration = asyncio.get_running_loop().time() - t0
            logger.error("LocalActionExecutor: Failed to spawn subprocess | error=%s", exc)
            return ExecutionResult(-3, "", f"Spawning error: {exc}", duration)

