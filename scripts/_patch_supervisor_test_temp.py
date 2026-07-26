from pathlib import Path

path = Path("tester/target_supervisor_tester.py")
text = path.read_text(encoding="utf-8")
text = text.replace("    ManagedServiceV1,\n", "", 1)
old = '''    def launch(self, spec):
        self.events.append(f"launch:{spec.service_name}")
        process = FakeProcess(
'''
new = '''    def launch(self, spec):
        self.events.append(f"launch:{spec.service_name}")
        spec.health_file.parent.mkdir(parents=True, exist_ok=True)
        spec.health_file.touch()
        process = FakeProcess(
'''
if text.count(old) != 1:
    raise SystemExit("expected one FakeLauncher.launch block")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
