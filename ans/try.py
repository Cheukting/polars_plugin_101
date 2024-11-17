import polars as pl
from polars_plugin_101 import *

df = pl.DataFrame(
    {
        "attendees": ["john", "mary", "connor", "sally"],
    }
)
out = df.with_columns(names=capitalize("attendees"))

print(out)

# out_next = out.with_columns(all_folks=cum_str("names"))
out_next = out.with_columns(all_folks=cum_str("names", sep=", ", trim=True))

print(out_next)

####

df2 = pl.DataFrame(
    {
        "temp_in_c": [10.0, 15.0, 16.0, 14.0],
    }
)
out2 = df2.with_columns(temp_in_f=to_farenheit("temp_in_c"))

print(out2)

####

df3 = pl.DataFrame(
    {
        "score_1": [10, 25, 76, 14],
        "score_2": [9, 36, 16, 14],
        "score_3": [80, 16, 66, 24],
    }
)
out3 = df3.with_columns(higher=larger("score_1", "score_2"))
out3_again = df3.with_columns(highest=largest("score_1", "score_2", "score_3"))

print(out3)
print(out3_again)

###

df4 = pl.DataFrame(
    {
        "first": ["John", "Mary", "Connor", "Sally"],
        "last": ["Smith", "Johnson", "McDonald", "Jones"],
        # "attendees": [["John", "Smith"], ["Mary", "Johnson"], ["Connor", "McDonald"], ["Sally", "Jones"]],
    }
)
out4 = df4.with_columns(all_folks=cum_str_mul("first", "last"))

print(out4)
