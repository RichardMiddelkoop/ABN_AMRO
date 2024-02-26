
def initial_function(x):
    return x.upper()

def test_initial_function():
    assert initial_function('abn_amro') == 'ABN_AMRO'