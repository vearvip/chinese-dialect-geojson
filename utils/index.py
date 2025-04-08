from utils.constant import YIN_DIAN_FEN_QV, JIAN_CHENG

def transform_dialect_infos_to_tree(dialect_infos):
    tree = {}

    # Helper function to add dialect nodes recursively
    def add_dialect_node(levels, short_name, node, path_so_far=""):
        if not levels:
            return

        current_level = levels.pop(0)
        full_path = f"{path_so_far}-{current_level}" if path_so_far else current_level

        if current_level not in node:
            node[current_level] = {
                "title": current_level,
                "value": full_path,
                "dialects": []
            }

        # Add the short name of the language to the current level's dialects array if it's not already there.
        if short_name not in node[current_level]["dialects"]:
            node[current_level]["dialects"].append(short_name)

        # Recursively process the remaining levels.
        if levels:
            if "children" not in node[current_level]:
                node[current_level]["children"] = {}
            add_dialect_node(levels, short_name, node[current_level]["children"], full_path)

    print(
      len(dialect_infos),
      dialect_infos[500][YIN_DIAN_FEN_QV],
      type(dialect_infos)
    )
    # Process each dialect info
    for dialect_info in dialect_infos: 
        dialect_levels = dialect_info[YIN_DIAN_FEN_QV].split("-")
        language_short_name = dialect_info[JIAN_CHENG]

        add_dialect_node(dialect_levels, language_short_name, tree)

    # Helper function to clean up empty children
    def clean_up_empty_children(node):
        if "children" in node:
            # Clean up each child recursively
            node["children"] = [clean_up_empty_children(child) for child in node["children"].values()]

            # Remove the children property if it's empty
            if not node["children"]:
                del node["children"]
        return node

    # Convert the tree object into a list and clean up any empty children
    result = [clean_up_empty_children(value) for value in tree.values()]

    # Helper function to sort dialects recursively
    def sort_dialects_recursively(node):
        if "dialects" in node:
            node["dialects"].sort()
        if "children" in node:
            for child in node["children"]:
                sort_dialects_recursively(child)

    # Sort the dialects in each node for consistency (optional)
    for node in result:
        sort_dialects_recursively(node)

    return result

 