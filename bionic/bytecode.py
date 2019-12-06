import re
from io import StringIO


def bytecode_from_func(func):
    # I'm not sure if this module works in non-CPython versions of Python, so
    # we'll import it only when this function is called.
    import dis

    buf = StringIO()
    dis.dis(func, file=buf)
    return buf.getvalue()


CODE_OBJECT_PATTERN = re.compile('<code.*>')


def canonicalize_bytecode(bytecode_str):
    canon_strs_by_long_str = {}

    modified_lines = []
    for line in bytecode_str.splitlines():
        if not line.strip():
            continue

        if line.startswith('Disassembly'):
            pass
        elif not line.startswith('    '):
            line = line.split(None, 1)[1]
        else:
            line = line.lstrip()

        match = CODE_OBJECT_PATTERN.search(line)
        if match is not None:
            long_str = match.group(0)

            if long_str in canon_strs_by_long_str:
                canon_str = canon_strs_by_long_str[long_str]
            else:
                canon_str = f'<code ref #{len(canon_strs_by_long_str)}>'
                canon_strs_by_long_str[long_str] = canon_str

            line = line.replace(long_str, canon_str)

        modified_lines.append(line)

    return '\n'.join(modified_lines)


def canonical_bytecode_bytes_from_func(func):
    return canonicalize_bytecode(bytecode_from_func(func)).encode('utf8')
