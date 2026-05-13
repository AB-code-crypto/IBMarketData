class SignalDataNotReadyError(RuntimeError):
    """Что делает: обозначает штатную нехватку данных для расчёта signal-window.
    Зачем нужна: signal-сервис должен пропускать такие расчёты, а не падать."""
