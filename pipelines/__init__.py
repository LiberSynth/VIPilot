"""
Пакет пайплайнов.

Регистрирует post-import hook через sys.meta_path: после загрузки любого
pipeline-модуля (кроме base.py) в его пространство имён подставляется
guard-обёртка для print, которая вызывает AssertionError при прямом использовании.

Единственный разрешённый способ логирования в файлах pipelines/ — pipeline_log.
"""

import sys

from pipelines.base import _forbidden_print

_GUARDED_MODULES = frozenset({
    'pipelines.planning',
    'pipelines.cleanup',
    'pipelines.story',
    'pipelines.video',
    'pipelines.transcode',
    'pipelines.publish',
})

_currently_loading = set()


def _apply_guards_to(mod):
    """Устанавливает guard-обёртку в пространство имён модуля."""
    mod.__dict__['print'] = _forbidden_print


class _PipelineLogGuard:
    """sys.meta_path finder: после загрузки pipeline-модуля применяет guard-обёртку."""

    def find_spec(self, fullname, path, target=None):
        if fullname not in _GUARDED_MODULES:
            return None
        if fullname in sys.modules:
            _apply_guards_to(sys.modules[fullname])
            return None
        if fullname in _currently_loading:
            return None
        import importlib.util
        _currently_loading.add(fullname)
        try:
            spec = importlib.util.find_spec(fullname)
        finally:
            _currently_loading.discard(fullname)
        if spec is None:
            return None
        original_loader = spec.loader

        class _GuardedLoader:
            def create_module(self, s):
                if hasattr(original_loader, 'create_module'):
                    return original_loader.create_module(s)
                return None

            def exec_module(self, module):
                original_loader.exec_module(module)
                _apply_guards_to(module)

        spec.loader = _GuardedLoader()
        return spec


def _install_guard():
    guard = _PipelineLogGuard()
    if not any(isinstance(f, _PipelineLogGuard) for f in sys.meta_path):
        sys.meta_path.insert(0, guard)
    for mod_name in _GUARDED_MODULES:
        mod = sys.modules.get(mod_name)
        if mod is not None:
            _apply_guards_to(mod)


_install_guard()
