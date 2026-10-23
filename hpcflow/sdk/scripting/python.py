from typing import ClassVar
from textwrap import dedent, indent

from hpcflow.sdk.scripting.writer import ScriptWriter
from hpcflow.sdk.utils.strings import extract_py_from_future_imports


class PythonScriptWriter(ScriptWriter):

    # TODO: do all script gen here - not just worker scripts?
    # TODO: [after jobscript-executor PR!] allow actions to share source scripts i.e. optionally pass function name
    #   - <<script:main_script_test_direct_in_direct_out.py::main>>

    _language: ClassVar[str] = "python"

    def compose_worker(self) -> str:

        TAB_INDENT = "    "
        script_funcs_lst: list[str] = []
        future_imports: set[str] = set()

        for act_idx, act in self.actions.items():

            func_name, snip_path = act.get_script_artifact_name(
                env_spec=self.env_spec,
                act_idx=act_idx,
                ret_specifiers=False,
                include_suffix=False,
                specs_suffix_delim="_",  # can't use "." in function name
            )
            main_func_name = snip_path.stem
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

        future_imports_str = (
            f"from __future__ import {', '.join(sorted(future_imports))}\n\n"
            if future_imports
            else ""
        )
        main = 'raise NotImplementedError("The script-worker execution protocol is not implemented.")'
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
