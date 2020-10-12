from typing import Dict, List


def get_tags_added(tag_res: Dict[str, List[str]]) -> List[str]:
    """Extracts the tags that were added to an entity
    
    :param tag_res: a tag results dictionary
    :returns: a list of tags that were successfully added
    """
    tags_added = []
    for tag, status in zip(tag_res['tags'], tag_res['tag_statuses']):
        if status in ['added', 'exists']:
            tags_added.append(tag)
    return tags_added
