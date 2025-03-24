# Rating scales (lower = better)
MOODYS_SCALE = {
    "Aaa": 1, "Aa1": 2, "Aa2": 3, "Aa3": 4,
    "A1": 5, "A2": 6, "A3": 7,
    "Baa1": 8, "Baa2": 9, "Baa3": 10,
    "Ba1": 11, "Ba2": 12, "Ba3": 13,
    "B1": 14, "B2": 15, "B3": 16,
    "Caa1": 17, "Caa2": 18, "Caa3": 19,
    "Ca": 20, "C": 21
}

FITCH_SCALE = S_AND_P_SCALE = {
    "AAA": 1, "AA+": 2, "AA": 3, "AA-": 4,
    "A+": 5, "A": 6, "A-": 7,
    "BBB+": 8, "BBB": 9, "BBB-": 10,
    "BB+": 11, "BB": 12, "BB-": 13,
    "B+": 14, "B": 15, "B-": 16,
    "CCC+": 17, "CCC": 18, "CCC-": 19,
    "CC": 20, "C": 21, "D": 22
}


if col_idx >= 3 and isinstance(value, str) and "→" in value:
    left, right = value.split("→")
    left = left.strip().replace("(", "").replace(")", "")
    right = right.strip().replace("(", "").replace(")", "")
    
    # Select appropriate scale
    scale = None
    if col_idx == 3:
        scale = MOODYS_SCALE
    elif col_idx == 4:
        scale = FITCH_SCALE
    elif col_idx == 5:
        scale = S_AND_P_SCALE

    # Compare if both sides are valid
    score_left = scale.get(left)
    score_right = scale.get(right)
    if score_left is not None and score_right is not None:
        if score_left < score_right:
            cell.fill = downgrade_fill
        elif score_left > score_right:
            cell.fill = upgrade_fill
        else:
            cell.fill = white_fill
    else:
        cell.fill = white_fill
else:
    cell.fill = white_fill