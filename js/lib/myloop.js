function myloop(ar, k) {
        return function loop() {
            var recur = loop;
            var arø2 = ar;
            var kø2 = k;
            do {
                recur = isEqual(kø2, 0) ? arø2 : (loop[0] = conj_js(arø2, kø2), loop[1] = kø2 - 1, loop);
            } while (arø2 = loop[0], kø2 = loop[1], recur === loop);
            return recur;
        }.call(this);
    };