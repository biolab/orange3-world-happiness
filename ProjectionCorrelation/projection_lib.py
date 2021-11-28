from sklearn.preprocessing import normalize, PowerTransformer
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics.pairwise import euclidean_distances
import numpy as np
import random
from math import sqrt
from Orange.data import ContinuousVariable

from whstudy import WorldIndicators

T = 1000


def score_projections(reference, x, y, overlay):
    """ Receives data (reference) and
    :return: list of (feature, score) pairs for all
    continuous features in overlay data
    """

    # 1. Normalize overlay data
    overlay_norm = normalize(overlay)

    if T < len(reference):
        ref_list = random.sample(reference, T)
    else:
        ref_list = reference

    # 2. Find nearest neighbours and for each find euclidean weight in
    # x-y-projection

    k = sqrt(len(reference))
    nbrs = NearestNeighbors(n_neighbors=k).fit(ref_list)

    distances, indices = nbrs.kneighbors(ref_list)

    # Transformer for Gaussian
    pt = PowerTransformer(method="yeo-johnson")

    # 2D table for scores of features in overlay
    score = np.zeros([len(overlay.columns), len(ref_list)])

    ind = 0
    for r in ref_list:
        nbr_ind = indices[ind]
        r_nbrs = ref_list[nbr_ind]
        w = euclidean_distances(r_nbrs[:, [x, y]], [r[x, y]])
        w = pt.fit(w)

        f_ind = 0
        for f in overlay.columns:
            if isinstance(f, ContinuousVariable):
                f_score = w * (overlay[ind, f] - overlay[nbr_ind, f])**2
                score[ind, f_ind] = np.mean(f_score)
                f_ind += 1
        ind += 1

    out = []
    f_ind = 0
    for f in overlay.columns:
        out.append((f, np.mean(score[f_ind, :])))

    return out


if __name__ == "__main__":
    handle = WorldIndicators('main', 'biolab')
    indicator_codes = [code for code, _, _, _ in handle.indicators()]
    country_codes = [code for code, _ in handle.countries()]

    wh_data = handle.data(random.sample(country_codes, 100), random.sample(indicator_codes, 100), range(2008, 2016))
    print(wh_data)

