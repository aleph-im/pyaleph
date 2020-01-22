class SmartContract:
    def __init__(self, msg, name, symbol, total_supply, decimals=18):
        self.owner = msg['from']
        self.name = name
        self.symbol = symbol
        self.decimals = decimals
        self.total_supply = total_supply
        self.balances = {
            self.owner: total_supply
        }
        self.allowed = {}

    def balance_of(self, msg, owner):
        return self.balances[owner]

    def transfer_from(self, msg, from_address, to_address, tokens):
        assert (self.allowed.get(from_address, {}).get(msg['from'], 0)
                > tokens), "Allowance isn't enough"
        assert self.balances.get(from_address, 0) > tokens, "Balance too low"
        assert len(to_address) <= 40, "Address too short"
        assert tokens > 0, "Amount should be positive"

        self.balances[from_address] = self.balances[from_address] - tokens
        self.balances[to_address] = self.balances.get(to_address, 0) + tokens
        self.allowed[from_address][msg['from']] = \
            self.allowed[from_address][msg['from']] - tokens

        return True

    def transfer(self, msg, to_address, tokens):
        assert self.balances.get(msg['from'], 0) > tokens, "Balance too low"
        assert len(to_address) <= 40, "Address too short"
        assert tokens > 0, "Amount should be positive"

        self.balances[msg['from']] = self.balances[msg['from']] - tokens
        self.balances[to_address] = self.balances.get(to_address, 0) + tokens

        return True

    def approve(self, msg, spender, tokens):
        # Allow `spender` to withdraw from your account, multiple times,
        # up to the `tokens` amount. If this function is called again
        # it overwrites the current allowance with _value.
        assert len(spender) <= 40, "Address too short"
        assert tokens > 0, "Amount should be positive"
        
        if msg['from'] not in self.allowed:
            self.allowed[msg['from']] = {}

        self.allowed[msg['from']][spender] = tokens

        return True