"""Keep pytest from collecting the pristine baseline snapshots under orig/.

orig/ holds unmodified copies of the repo files this patch changes (the diff/transform
baseline). orig/test_asl_payload_contract.py has the same basename as the real repo test, so
without this ignore pytest raises an import-file-mismatch collection error. These snapshots are
data, not tests.
"""
collect_ignore_glob = ["orig/*"]
