"""
Order-input normalization tests for copier_server.py (the copier that actually
serves :7332). Mirrors futuresforged-bot PR #9's test_order_normalization.py, but
targets THIS file — the deployed one — not copier_engine.py.

Run:  python3 -m unittest test_copier_normalization -v
Importing copier_server is side-effect-free (the server only starts under
__main__); it reads copier_state.json read-only at import.
"""
import unittest
import copier_server as cs


class TestCanon(unittest.TestCase):
    def test_direction_chart_studio_vocab(self):
        self.assertEqual(cs.canon_direction("BUY"), "LONG")
        self.assertEqual(cs.canon_direction("sell"), "SHORT")

    def test_direction_dashboard_vocab_passthrough(self):
        self.assertEqual(cs.canon_direction("LONG"), "LONG")
        self.assertEqual(cs.canon_direction("SHORT"), "SHORT")

    def test_direction_unknown_is_none(self):
        self.assertIsNone(cs.canon_direction("UP"))
        self.assertIsNone(cs.canon_direction(""))
        self.assertIsNone(cs.canon_direction(None))

    def test_order_type_mkt_disconnect(self):
        # The bug this fix closes: "MKT" must canonicalize to "Market" so the
        # live submit branch (otype=="market") recognizes it instead of failing closed.
        self.assertEqual(cs.canon_order_type("MKT"), "Market")
        self.assertEqual(cs.canon_order_type("LMT"), "Limit")
        self.assertEqual(cs.canon_order_type("STP"), "Stop Market")
        self.assertEqual(cs.canon_order_type("Market"), "Market")

    def test_order_type_unknown_is_none(self):
        self.assertIsNone(cs.canon_order_type("BANANA"))


class TestNormalizeOrder(unittest.TestCase):
    def test_chart_studio_buy_market(self):
        o, err = cs.normalize_order(
            {"side": "BUY", "direction": "BUY", "instrument": "MNQ",
             "contracts": 1, "order_type": "MKT"})
        self.assertIsNone(err)
        self.assertEqual(o["direction"], "LONG")
        self.assertEqual(o["order_type"], "Market")
        self.assertEqual(o["contracts"], 1)

    def test_side_only_no_direction(self):
        o, err = cs.normalize_order({"side": "SELL", "instrument": "MNQ", "order_type": "MKT"})
        self.assertIsNone(err)
        self.assertEqual(o["direction"], "SHORT")

    def test_bad_direction_rejected(self):
        o, err = cs.normalize_order({"direction": "UP", "instrument": "NQ", "contracts": 1, "order_type": "MKT"})
        self.assertIsNone(o)
        self.assertIn("Invalid direction", err)

    def test_bad_order_type_rejected(self):
        o, err = cs.normalize_order({"direction": "BUY", "instrument": "NQ", "contracts": 1, "order_type": "BANANA"})
        self.assertIsNone(o)
        self.assertIn("Invalid order type", err)

    def test_limit_without_price_rejected(self):
        o, err = cs.normalize_order({"direction": "BUY", "instrument": "NQ", "contracts": 1, "order_type": "LMT"})
        self.assertIsNone(o)
        self.assertIn("Limit price required", err)

    def test_limit_with_price_ok(self):
        o, err = cs.normalize_order(
            {"direction": "BUY", "instrument": "NQ", "contracts": 2, "order_type": "LMT", "limit_price": 21450.25})
        self.assertIsNone(err)
        self.assertEqual(o["order_type"], "Limit")
        self.assertEqual(o["contracts"], 2)

    def test_contracts_coerced_and_floored(self):
        o, err = cs.normalize_order({"direction": "BUY", "instrument": "MNQ", "contracts": 0, "order_type": "MKT"})
        self.assertIsNone(err)
        self.assertEqual(o["contracts"], 1)  # max(1, 0)


if __name__ == "__main__":
    unittest.main()
