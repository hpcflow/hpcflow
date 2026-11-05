from typing import ClassVar
from textwrap import dedent, indent

from hpcflow.sdk.scripting.writer import ScriptWriter
from hpcflow.sdk.utils.strings import extract_py_from_future_imports


class PythonScriptWriter(ScriptWriter):
    """Generate Python wrappers and a registry for assigned workflow actions."""

    # TODO: do all script gen here - not just worker scripts?
    # TODO: [after jobscript-executor PR!] allow actions to share source scripts i.e. optionally pass function name
    #   - <<script:main_script_test_direct_in_direct_out.py::main>>

    _language: ClassVar[str] = "python"

    def compose_worker(self) -> str:
        """Compose a Python module with registered wrappers and bootstrap entry point."""

        TAB_INDENT = "    "
        script_funcs_lst: list[str] = []
        future_imports: set[str] = set()
        registry: list[str] = []

        for wrapper_idx, (action_key, act) in enumerate(self.actions.items()):

            func_name, snip_path = act.get_script_artifact_name(
                env_spec=self.env_spec,
                act_idx=wrapper_idx,
                ret_specifiers=False,
                include_suffix=False,
                specs_suffix_delim="_",  # can't use "." in function name
            )
            main_func_name = snip_path.stem
            registry.append(f"{action_key!r}: {func_name}")
            with snip_path.open("rt") as fp:
                script_str = fp.read()

            script_str, future_imports_i = extract_py_from_future_imports(script_str)
            future_imports.update(future_imports_i)
            script_funcs_lst.append(
                dedent("""\
                def {act_name}(*args, **kwargs):
                {script_str}
                    return {main_func_name}(*args, **kwargs)
                """).format(
                    act_name=func_name,
                    script_str=indent(script_str, TAB_INDENT),
                    main_func_name=main_func_name,
                )
            )

        script_funcs = "\n".join(script_funcs_lst)
        script_funcs += "\nACTION_SCRIPTS = {" + ", ".join(registry) + "}\n"

        future_imports_str = (
            f"from __future__ import {', '.join(sorted(future_imports))}\n\n"
            if future_imports
            else ""
        )
        main = dedent("""\
            from hpcflow.sdk.execution.script_worker.bootstrap import (
                run_generated_script_worker,
            )

            run_generated_script_worker(__file__, ACTION_SCRIPTS)
        """).rstrip()
        script = dedent("""\
            {future_imports_str}{script_funcs}
            if __name__ == "__main__":
            {main}
        """).format(
            future_imports_str=future_imports_str,
            script_funcs=script_funcs,
            main=indent(main, TAB_INDENT),
        )

        return script
