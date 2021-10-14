class SmartContract:
    def __init__(self, msg, name, symbol, total_supply, decimals=18):
        self.owner = msg["sender"]
        self.name = name
        self.symbol = symbol
        self.decimals = decimals
        self.total_supply = total_supply * (10 ** decimals)
        self.balances = {self.owner: self.total_supply}
        self.allowed = {}

    def balance_of(self, msg, owner):
        return self.balances[owner]

    def transfer_from(self, msg, from_address, to_address, tokens):
        assert (
            self.allowed.get(from_address, {}).get(msg["from"], 0) > tokens
        ), "Allowance isn't enough"
        assert self.balances.get(from_address, 0) > tokens, "Balance too low"
        assert len(to_address) <= 40, "Address too long"
        assert tokens > 0, "Amount should be positive"

        self.balances[from_address] = self.balances[from_address] - tokens
        self.balances[to_address] = self.balances.get(to_address, 0) + tokens
        self.allowed[from_address][msg["from"]] = (
            self.allowed[from_address][msg["from"]] - tokens
        )

        return True

    def transfer(self, msg, to_address, tokens):
        assert self.balances.get(msg["sender"], 0) > tokens, "Balance too low"
        assert len(to_address) <= 40, "Address too long"
        assert tokens > 0, "Amount should be positive"

        self.balances[msg["sender"]] = self.balances[msg["sender"]] - tokens
        self.balances[to_address] = self.balances.get(to_address, 0) + tokens

        return True

    def approve(self, msg, spender, tokens):
        # Allow `spender` to withdraw from your account, multiple times,
        # up to the `tokens` amount. If this function is called again
        # it overwrites the current allowance with _value.
        assert len(spender) <= 40, "Address too long"
        assert tokens > 0, "Amount should be positive"

        if msg["sender"] not in self.allowed:
            self.allowed[msg["sender"]] = {}

        self.allowed[msg["sender"]][spender] = tokens

        return True
