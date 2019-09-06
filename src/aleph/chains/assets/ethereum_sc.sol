pragma solidity ^0.5.11;

contract AlephSync{

    event Emit(uint256 timestamp, address addr, string message); 
    //emitter.doEmit("blah", {from: "[primary acc]", value: web3.toWei(100, "ether")});
    
    function doEmit(string memory message) public {
        emit Emit(block.timestamp, msg.sender, message);
    }

}
