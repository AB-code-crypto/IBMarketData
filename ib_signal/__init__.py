"""Runtime helpers for the ib_signal package."""

import importlib.abc
import importlib.machinery
import sys
from types import ModuleType
from typing import Any

_SIGNAL_PLOT_MODULE_NAME = "ib_signal.signal_plot"
_SIGNAL_PLOT_PATCH_MARKER = "_candidate_funnel_dedupe_patch_installed"


def _candidate_funnel_labels(rows: list[tuple[str, str | None]] | None) -> set[str]:
    """Возвращает labels строк из CANDIDATE FUNNEL для скрытия дублей выше по панели."""
    labels: set[str] = set()

    for text, _ in rows or []:
        labels.add(str(text).split(":", 1)[0].strip())

    return labels


def _filter_signal_plot_info_rows(
        title: str,
        rows: list[tuple[str, str | None]],
        candidate_funnel_labels: set[str],
) -> list[tuple[str, str | None]]:
    """Оставляет в верхних блоках только то, чего нет в CANDIDATE FUNNEL."""
    if not candidate_funnel_labels:
        return rows

    if title == "REGRESSION (bps / pt)" and "dirs p/sma" in candidate_funnel_labels:
        return [
            (text, color)
            for text, color in rows
            if not str(text).strip().startswith(("price  dir", "sma 600 dir"))
        ]

    if title == "RELATION (bps / pt)" and "relation" in candidate_funnel_labels:
        return [
            (text, color)
            for text, color in rows
            if not str(text).strip().startswith("price/sma 600")
        ]

    if title == "POTENTIAL (pt)" and "potential" in candidate_funnel_labels:
        return [
            (text, color)
            for text, color in rows
            if not str(text).strip().startswith(("used", "status"))
        ]

    return rows


def _patch_signal_plot(module: ModuleType) -> None:
    """Патчит только PNG info-panel: Candidate Funnel остаётся источником повторяющихся этапов."""
    if getattr(module, _SIGNAL_PLOT_PATCH_MARKER, False):
        return

    original_save_signal_candidate_plot = module.save_signal_candidate_plot
    original_draw_info_section = module.draw_info_section
    patch_state: dict[str, set[str]] = {"candidate_funnel_labels": set()}

    def patched_draw_info_section(
            ax_info: Any,
            *,
            title: str,
            rows: list[tuple[str, str | None]],
            y: float,
            line_height: float,
    ) -> float:
        filtered_rows = _filter_signal_plot_info_rows(
            title=title,
            rows=rows,
            candidate_funnel_labels=patch_state["candidate_funnel_labels"],
        )

        if not filtered_rows:
            return y

        return original_draw_info_section(
            ax_info,
            title=title,
            rows=filtered_rows,
            y=y,
            line_height=line_height,
        )

    def patched_save_signal_candidate_plot(*args: Any, **kwargs: Any):
        previous_labels = patch_state["candidate_funnel_labels"]
        patch_state["candidate_funnel_labels"] = _candidate_funnel_labels(
            kwargs.get("candidate_funnel_rows"),
        )
        try:
            return original_save_signal_candidate_plot(*args, **kwargs)
        finally:
            patch_state["candidate_funnel_labels"] = previous_labels

    module.draw_info_section = patched_draw_info_section
    module.save_signal_candidate_plot = patched_save_signal_candidate_plot
    setattr(module, _SIGNAL_PLOT_PATCH_MARKER, True)


class _SignalPlotPatchLoader(importlib.abc.Loader):
    def __init__(self, wrapped_loader: importlib.abc.Loader) -> None:
        self.wrapped_loader = wrapped_loader

    def create_module(self, spec):
        create_module = getattr(self.wrapped_loader, "create_module", None)
        if create_module is None:
            return None
        return create_module(spec)

    def exec_module(self, module: ModuleType) -> None:
        self.wrapped_loader.exec_module(module)
        _patch_signal_plot(module)


class _SignalPlotPatchFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path: Any, target: Any = None):
        if fullname != _SIGNAL_PLOT_MODULE_NAME:
            return None

        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is None or spec.loader is None:
            return None

        spec.loader = _SignalPlotPatchLoader(spec.loader)
        return spec


def _install_signal_plot_patch_finder() -> None:
    if _SIGNAL_PLOT_MODULE_NAME in sys.modules:
        _patch_signal_plot(sys.modules[_SIGNAL_PLOT_MODULE_NAME])
        return

    if not any(isinstance(finder, _SignalPlotPatchFinder) for finder in sys.meta_path):
        sys.meta_path.insert(0, _SignalPlotPatchFinder())


_install_signal_plot_patch_finder()
