import http from 'k6/http';
import { sleep } from 'k6';
import crypto from 'k6/crypto';

export const options = {
    vus: 10,
    iterations: 20,
};

export default function () {
    var toParse = "https://www.data.gouv.fr/fr/datasets/r/e3d83ab3-dc52-4c99-abaf-8a38050cc68c"
    var base = "https://csvapi.data.gouv.fr";

    // change me to invalidate cache
    var rdm = "2";
    let toApify = `${toParse}?ts=${rdm}`
    let hash = crypto.md5(toApify, 'hex');
    console.log(hash);

    // apify 1
    var apify = `${base}/apify?url=${toApify}`;
    http.get(apify);

    // analyze 1
    var analyze = `${base}/apify?analysis=yes&url=${toApify}`;
    http.get(analyze);

    // make 10 requests
    for (let id = 1; id <= 10; id++) {
        http.get("https://csvapi.data.gouv.fr/api/26bdf0d090dfbaecbe213c6f551a46ac", {
            tags: { name: 'request' },
        });
        sleep(0.1);
    }
}
