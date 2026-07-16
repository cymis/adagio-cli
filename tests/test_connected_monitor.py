import json
import unittest
import urllib.error
from unittest.mock import patch

from adagio.monitor.connected import ConnectedMonitor


class _FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _captured_payloads(calls):
    payloads = []
    for call in calls:
        req = call.args[0]
        payloads.append(json.loads(req.data.decode("utf-8")))
    return payloads


class ConnectedMonitorEventTests(unittest.TestCase):
    def _monitor(self, **kwargs) -> ConnectedMonitor:
        return ConnectedMonitor(
            runtime_url="http://127.0.0.1:9999/api", job_id="job-1", **kwargs
        )

    def test_pulling_and_starting_container_events(self) -> None:
        monitor = self._monitor()
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.pulling_image(task_id="t1", image_ref="img:1")
            monitor.starting_container(task_id="t1", image_ref="img:1")

        payloads = _captured_payloads(urlopen.call_args_list)
        self.assertEqual(payloads[0]["event"], "pulling_image")
        self.assertEqual(payloads[0]["image_ref"], "img:1")
        self.assertEqual(payloads[1]["event"], "starting_container")

    def test_finish_task_carries_enrichment_fields(self) -> None:
        monitor = self._monitor()
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.finish_task(
                task_id="t1",
                status="completed",
                command=["docker", "run", "img"],
                exit_code=0,
                image_ref="img:1",
                image_digest="img@sha256:abc",
                reused=False,
                input_signature="sha256:sig",
                timings={"run_seconds": 1.5},
                log_path="/logs/t1_container.log",
                traceback="Traceback (most recent call last):\nRuntimeError: exploded",
            )
        payload = _captured_payloads(urlopen.call_args_list)[0]
        self.assertEqual(payload["event"], "task_finished")
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["command"], ["docker", "run", "img"])
        self.assertEqual(payload["exit_code"], 0)
        self.assertEqual(payload["image_ref"], "img:1")
        self.assertEqual(payload["image_digest"], "img@sha256:abc")
        self.assertEqual(payload["input_signature"], "sha256:sig")
        self.assertEqual(payload["timings"], {"run_seconds": 1.5})
        self.assertEqual(payload["log_path"], "/logs/t1_container.log")
        self.assertEqual(
            payload["traceback"],
            "Traceback (most recent call last):\nRuntimeError: exploded",
        )
        self.assertFalse(payload["reused"])

    def test_reproducibility_event(self) -> None:
        monitor = self._monitor()
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.report_reproducibility(
                reproducibility={"adagio_version": "9.9.9", "image_digests": {}}
            )
        payload = _captured_payloads(urlopen.call_args_list)[0]
        self.assertEqual(payload["event"], "reproducibility")
        self.assertEqual(payload["reproducibility"]["adagio_version"], "9.9.9")

    def test_output_saved_carries_log_path(self) -> None:
        monitor = self._monitor()
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.finish_output(
                output_id="o1",
                output_name="table",
                destination="/out/table.qza",
                log_path="/logs/t1_container.log",
            )
        payload = _captured_payloads(urlopen.call_args_list)[0]
        self.assertEqual(payload["event"], "output_saved")
        self.assertEqual(payload["log_path"], "/logs/t1_container.log")


class ConnectedMonitorTransportTests(unittest.TestCase):
    def _monitor(self, **kwargs) -> ConnectedMonitor:
        return ConnectedMonitor(
            runtime_url="http://127.0.0.1:9999/api", job_id="job-1", **kwargs
        )

    def test_authorization_header_sent_when_token_present(self) -> None:
        monitor = self._monitor(token="secret-token")
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.start_pipeline(total_tasks=1)
        req = urlopen.call_args_list[0].args[0]
        self.assertEqual(req.headers.get("Authorization"), "Bearer secret-token")

    def test_no_authorization_header_without_token(self) -> None:
        monitor = self._monitor(token=None)
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.start_pipeline(total_tasks=1)
        req = urlopen.call_args_list[0].args[0]
        self.assertIsNone(req.headers.get("Authorization"))

    def test_token_read_from_env(self) -> None:
        with patch.dict("os.environ", {"RUNTIME_TOKEN": "env-token"}):
            monitor = self._monitor()
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.start_pipeline(total_tasks=1)
        req = urlopen.call_args_list[0].args[0]
        self.assertEqual(req.headers.get("Authorization"), "Bearer env-token")

    def test_transient_failure_buffers_then_redrains(self) -> None:
        monitor = self._monitor(max_retries=0)
        # First post fails (adapter down); the event is buffered, not dropped.
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            side_effect=urllib.error.URLError("down"),
        ):
            monitor.start_pipeline(total_tasks=1)

        # Adapter recovers: the next post drains the buffered event first.
        with patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            return_value=_FakeResponse(),
        ) as urlopen:
            monitor.finish_pipeline()

        payloads = _captured_payloads(urlopen.call_args_list)
        events = [p["event"] for p in payloads]
        self.assertEqual(events, ["pipeline_start", "pipeline_finish"])

    def test_retries_on_transient_failure(self) -> None:
        monitor = self._monitor(max_retries=2)
        with patch(
            "adagio.monitor.connected.time.sleep", return_value=None
        ), patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            side_effect=[
                urllib.error.URLError("x"),
                urllib.error.URLError("x"),
                _FakeResponse(),
            ],
        ) as urlopen:
            monitor.start_pipeline(total_tasks=1)
        # 1 initial + 2 retries = 3 attempts, then success (no buffering).
        self.assertEqual(urlopen.call_count, 3)

    def test_never_raises_on_permanent_failure(self) -> None:
        monitor = self._monitor(max_retries=1)
        with patch(
            "adagio.monitor.connected.time.sleep", return_value=None
        ), patch(
            "adagio.monitor.connected.urllib.request.urlopen",
            side_effect=urllib.error.URLError("down"),
        ):
            # Must not raise even though every attempt fails.
            monitor.start_pipeline(total_tasks=1)


if __name__ == "__main__":
    unittest.main()
