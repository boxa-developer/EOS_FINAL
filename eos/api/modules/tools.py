def correct_date(a):
    a = '-'.join([a.split('-')[i] if len(a.split('-')[i])>1 else f"0{a.split('-')[i]}" for i in range(len(a.split('-')))])
    return a

