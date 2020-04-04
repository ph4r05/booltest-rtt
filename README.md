# BoolTest RTT runner

Utility integrating BoolTest battery to the RTT


## Configuration

Environment variable `RTT_PARALLEL` precedes `toolkit-settings.execution.max-parallel-tests` in the configuration. 

Expected configuration in `rtt-settings.json`

```json
{
    "booltest": {
        "default-cli": "--no-summary --json-out --log-prints --top 128 --no-comb-and --only-top-comb --only-top-deg --no-term-map --topterm-heap --topterm-heap-k 256 --best-x-combs 512",
        "strategies": [
            {
                "name": "v1",
                "cli": "",
                "variations": [
                    {
                        "bl": [128, 256, 384, 512],
                        "deg": [1, 2, 3],
                        "cdeg": [1, 2, 3],
                        "exclusions": []
                    }
                ]
            },
            {
                "name": "halving",
                "cli": "--halving",
                "variations": [
                    {
                        "bl": [128, 256, 384, 512],
                        "deg": [1, 2, 3],
                        "cdeg": [1, 2, 3],
                        "exclusions": []
                    }
                ]
            }
        ]
    }
}
```
