import json
import unittest

from tools.filter_project_measure import parse_measurement_output, planned_phases, child_argv, validate_memory_record


def sample_record(case="wide-utf8-discard-mixed", mode="candidate"):
    fused = mode == "candidate"
    return {
        "record": "filter_project_single_sample",
        "case": case,
        "mode": mode,
        "elapsed_push_ns": 2345,
        "rows_seed": 4096,
        "source_input_rows_seed": 4096,
        "rows_change": 4096,
        "source_columns": 4,
        "selected_rows_seed": 1366,
        "selected_rows_change": 1366,
        "change_diff_negative_rows": 1366,
        "change_diff_zero_rows": 1365,
        "change_diff_positive_rows": 1365,
        "source_payload_bytes": 16_777_216,
        "actual_fused_kernel_entries_seed": 1 if fused else 0,
        "actual_fused_kernel_entries_before": 0,
        "actual_fused_kernel_entries_after": 1 if fused else 0,
        "actual_fused_kernel_entries_after_continuation": 2 if fused else 0,
        "actual_fused_kernel_entries_continuation_before": 1 if fused else 0,
        "changelog_rows": 10,
        "snapshot_rows": 1000,
        "view_noop": False,
        "continuation_changelog_rows": 2,
        "continuation_snapshot_rows": 1001,
        "checkpoint_bytes": 1024,
        "float_group_history_bits": [],
        "float_group_reference_sequence_bits": [],
        "float_group_push_history_bits": [],
        "allocation_observer_feature": False,
    }


class MeasurementOutputParserTest(unittest.TestCase):
    def test_memory_record_requires_actual_dimensions_schema_and_post_exec_hwm(self):
        record = {
            "record": "filter_project_memory_sample",
            "case": "wide-utf8-discard-mixed",
            "mode": "candidate",
            "source_input_rows_seed": 4096,
            "source_input_rows_change": 4096,
            "source_columns": 4,
            "source_payload_bytes_seed": 16_777_216,
            "source_payload_bytes_change": 16_777_216,
            "selected_rows_seed": 1366,
            "selected_rows_change": 1366,
            "source_schema_seed": [
                {"name": "predicate", "data_type": "Int64"},
                {"name": "value", "data_type": "Int64"},
                {"name": "payload", "data_type": "Utf8"},
                {"name": "discarded", "data_type": "Boolean"},
            ],
            "post_exec_observed_peak_rss_kib": 42000,
        }
        self.assertEqual(validate_memory_record(record, record["case"], record["mode"]), record)
        for key, value in (("source_input_rows_seed", 4095), ("source_payload_bytes_seed", 1), ("post_exec_observed_peak_rss_kib", None), ("source_schema_seed", [])):
            broken = dict(record, **{key: value})
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_memory_record(broken, record["case"], record["mode"])

    def test_child_cpu_pin_wraps_only_the_child_when_requested(self):
        binary = "/tmp/archived/test-binary"
        test = "core::tests::sample"
        self.assertEqual(child_argv(binary, test, None), [binary, "--ignored", "--exact", test, "--nocapture", "--test-threads=1"])
        self.assertEqual(child_argv(binary, test, 12), ["/usr/bin/taskset", "-c", "12", binary, "--ignored", "--exact", test, "--nocapture", "--test-threads=1"])

    def test_default_and_smoke_plan_all_main_phases(self):
        expected = {"excluded-warmup", "timing", "excluded-operator-warmup", "operator-wall", "allocation", "memory"}
        self.assertEqual(set(planned_phases(False)), expected)
        self.assertEqual(set(planned_phases(True)), expected)

    def test_accepts_rust_sorted_json_after_harness_prefix_lines(self):
        payload = json.dumps(sample_record(), sort_keys=True)
        self.assertEqual(
            parse_measurement_output(
                "running 1 test\ntest core::tests::sample ... " + payload + "\nok\ntest passed\n",
                "wide-utf8-discard-mixed",
                "candidate",
            ),
            json.loads(payload),
        )

    def test_rejects_duplicate_matching_record(self):
        payload = json.dumps(sample_record(), sort_keys=True)
        with self.assertRaises(ValueError):
            parse_measurement_output(payload + "\n" + payload, "wide-utf8-discard-mixed", "candidate")

    def test_selects_expected_record_not_another_json_object(self):
        other = json.dumps({"record": "unrelated", "case": "wide-utf8-discard-mixed"})
        expected = sample_record()
        expected["record"] = "filter_project_single_sample"
        self.assertEqual(
            parse_measurement_output(
                other + "\n" + json.dumps(expected, sort_keys=True),
                "wide-utf8-discard-mixed",
                "candidate",
            ),
            expected,
        )

    def test_rejects_wrong_case_mode_duration_and_fusion_counter(self):
        for change in (
            {"case": "narrow-mixed"},
            {"mode": "force-eager"},
            {"elapsed_push_ns": -1},
            {"actual_fused_kernel_entries_after": 0},
            {"actual_fused_kernel_entries_after_continuation": 1},
            {"allocation_observer_feature": True},
        ):
            record = sample_record()
            record["record"] = "filter_project_single_sample"
            record.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                parse_measurement_output(
                    json.dumps(record, sort_keys=True),
                    "wide-utf8-discard-mixed",
                    "candidate",
                )

    def test_rejects_empty_selected_changelog(self):
        record = sample_record()
        record["changelog_rows"] = 0
        with self.assertRaises(ValueError):
            parse_measurement_output(json.dumps(record), "wide-utf8-discard-mixed", "candidate")

    def test_accepts_raw_all_false_change_with_empty_changelog(self):
        record = sample_record("wide-utf8-discard-allfalse")
        record.update({"selected_rows_change": 0, "changelog_rows": 0, "view_noop": True})
        self.assertEqual(
            parse_measurement_output(json.dumps(record), record["case"], "candidate"),
            record,
        )

    def test_accepts_eager_and_dictionary_fallback_counters(self):
        eager = sample_record(mode="force-eager")
        self.assertEqual(parse_measurement_output(json.dumps(eager), eager["case"], "force-eager"), eager)
        fallback = sample_record("dictionary-discard-fallback")
        fallback.update({
            "rows_seed": 512,
            "source_input_rows_seed": 512,
            "rows_change": 512,
            "source_columns": 4,
            "source_payload_bytes": 0,
            "selected_rows_seed": 171,
            "selected_rows_change": 171,
            "change_diff_negative_rows": 171,
            "change_diff_zero_rows": 170,
            "change_diff_positive_rows": 171,
            "actual_fused_kernel_entries_seed": 0,
            "actual_fused_kernel_entries_after": 0,
            "actual_fused_kernel_entries_continuation_before": 0,
            "actual_fused_kernel_entries_after_continuation": 0,
        })
        self.assertEqual(parse_measurement_output(json.dumps(fallback), fallback["case"], "candidate"), fallback)


if __name__ == "__main__":
    unittest.main()
