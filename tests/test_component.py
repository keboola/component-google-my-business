"""
Created on 12. 11. 2018

@author: esner
"""

import json
import os
import tempfile
import unittest
from unittest import mock

from freezegun import freeze_time
from keboola.component.exceptions import UserException

from component import Component
from google_my_business import (
    GoogleMyBusiness,
    GoogleMyBusinessException,
    GoogleMyBusinessRequestError,
)


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestGetRequestErrorHandling(unittest.TestCase):
    """Regression tests for the defensive fix around unexpected HTTP statuses.

    A 503 (or any other unexpected non-200 that the existing retry logic cannot
    recover from) used to escape ``get_request`` as a bare ``Exception``, which the
    component boundary did not catch, so the job died with an opaque internal error
    (exit 2). It is now raised as ``GoogleMyBusinessRequestError`` and surfaced by the
    component as a clear ``UserException`` (exit 1) with the same message.
    """

    @staticmethod
    def _make_client():
        return GoogleMyBusiness(access_token="tok", data_folder_path=tempfile.gettempdir())

    @mock.patch("time.sleep")  # neutralise the backoff waits so the test is fast
    def test_unexpected_status_raises_request_error_after_retries(self, _sleep):
        gmb = self._make_client()
        resp = mock.Mock(status_code=503, text="Service Unavailable")
        with mock.patch.object(gmb.session, "get", return_value=resp) as mock_get:
            with self.assertRaises(GoogleMyBusinessRequestError) as ctx:
                gmb.get_request("https://example.test/resource")

        # Same message as before the fix - only the exception *type* changed.
        self.assertEqual(str(ctx.exception), "Request failed with status code 503")
        # Inner backoff retry budget (max_tries=7) is preserved.
        self.assertEqual(mock_get.call_count, 7)

    def test_success_and_handled_statuses_are_unchanged(self):
        """Happy path and the explicitly-handled statuses must behave byte-for-byte as before."""
        gmb = self._make_client()

        ok = mock.Mock(status_code=200)
        with mock.patch.object(gmb.session, "get", return_value=ok):
            self.assertEqual(gmb.get_request("https://example.test/ok"), (200, ok))

        for handled in (400, 401, 403, 404, 500, 501):
            resp = mock.Mock(status_code=handled)
            with mock.patch.object(gmb.session, "get", return_value=resp):
                self.assertEqual(gmb.get_request("https://example.test/handled"), (handled, resp))

    def test_request_error_bypasses_endpoint_retry_decorators(self):
        """The new exception must not be swept up by the per-endpoint backoff decorators.

        ``list_locations`` / ``list_media`` retry on ``GoogleMyBusinessException``. The
        terminal request error must therefore be an ``Exception`` (so ``get_request``'s
        own ``backoff.on_exception(..., Exception, ...)`` still retries it) but NOT a
        ``GoogleMyBusinessException`` (so the outer decorators keep ignoring it exactly
        as they ignored the previous bare ``Exception``). This is what keeps the retry
        behaviour identical.
        """
        self.assertTrue(issubclass(GoogleMyBusinessRequestError, Exception))
        self.assertFalse(issubclass(GoogleMyBusinessRequestError, GoogleMyBusinessException))

    def test_component_run_converts_request_error_to_user_exception(self):
        """End-to-end: a terminal request error now exits 1 (UserException), not exit 2."""
        with tempfile.TemporaryDirectory() as datadir:
            os.makedirs(os.path.join(datadir, "in"))
            os.makedirs(os.path.join(datadir, "out", "tables"))
            with open(os.path.join(datadir, "in", "state.json"), "w") as f:
                json.dump({}, f)
            config = {
                "parameters": {
                    "#api_token": "x",
                    "endpoints": ["reviews"],
                    "accounts": {"label": "acc", "value": "accounts/1"},
                    "request_range": {"start_date": "7 days ago", "end_date": "today"},
                },
                "authorization": {"oauth_api": {"credentials": {}}},
                "action": "run",
            }
            with open(os.path.join(datadir, "config.json"), "w") as f:
                json.dump(config, f)

            with mock.patch.dict(os.environ, {"KBC_DATADIR": datadir}):
                comp = Component()
                with (
                    mock.patch.object(Component, "get_oauth_token", return_value="tok"),
                    mock.patch.object(
                        GoogleMyBusiness,
                        "process",
                        side_effect=GoogleMyBusinessRequestError("Request failed with status code 503"),
                    ),
                ):
                    with self.assertRaises(UserException) as ctx:
                        comp.run()

        self.assertIn("Request failed with status code 503", str(ctx.exception))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
