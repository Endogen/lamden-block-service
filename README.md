[Install PostgreSQL on macOS](https://gist.github.com/phortuin/2fe698b6c741fd84357cec84219c6667)

Create database `lamden_mainnet`

Retrieve KEY or VALUE from state

```
select state::json->'key'
from current_state

select state::json->'value'
from current_state
```