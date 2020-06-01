import os
import shutil


def test_move_cache_files(builder, tmp_path):
    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    def xy(x, y):
        return x * y

    cur_dir = os.path.join(tmp_path, "current")
    new_dir = os.path.join(tmp_path, "new")

    builder.set("core__persistent_cache__flow_dir", cur_dir)
    flow = builder.build()
    # call a method to create cache
    assert flow.get("xy") == 6

    shutil.copytree(cur_dir, new_dir)

    builder.set("core__persistent_cache__flow_dir", new_dir)
    flow = builder.build()
    assert flow.get("xy") == 6
