import math

import Orange.data
import numpy as np
import random
from math import sqrt

from sklearn.preprocessing import normalize, QuantileTransformer
from sklearn.manifold import TSNE
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics.pairwise import euclidean_distances

from Orange.data import ContinuousVariable, Variable, Table, table_from_frame
from Orange.preprocess import Normalize

from whstudy import WorldIndicators

T = 1000

def gaussian_transform(x):
    std = np.std(x)
    x = x / std
    values = np.zeros(x.shape)
    for i in range(x.shape[0]):
        num = (-(x[i])**2) / 2
        values[i] = math.exp(num)
    return values


def score_projections(reference, x, y, overlay):
    """ Receives reference and overlay data and scores features in overlay based on x,y
    :param reference: Ordered data for weighting overlay features inlcudes x,y features
    :type reference: Table
    :param overlay: Ordered data on which we score the features
    :type overlay: Table
    :param x: dimension for weight calculation in reference
    :type x: Variable
    :param y: dimension for weight calculation in reference
    :type y: Variable
    :return: list of (feature, score) pairs for all continuous features
    """

    # 1. Normalize overlay data
    normalizer = Normalize(norm_type=Normalize.NormalizeBySpan, zero_based=True)
    overlay = normalizer(overlay)
    n_rows = reference.X.shape[0]

    if T < n_rows:
        ref_list = random.sample(reference, T)
        n_rows = T
    else:
        ref_list = reference

    # 2. Find nearest neighbours and for each find euclidean weight in x-y-projection

    k = round(sqrt(n_rows))
    nbrs = NearestNeighbors(n_neighbors=k).fit(ref_list.X_df)

    _, indices = nbrs.kneighbors(ref_list.X_df)

    # 2D table for scores of features in overlay
    score = np.zeros([overlay.X.shape[1], n_rows])

    ind = 0
    for r in ref_list:
        nbr_ind = indices[ind]
        r_nbrs = ref_list[nbr_ind]
        numpy_nbrs = r_nbrs.metas_df.loc[:, [x.name, y.name]].to_numpy()
        w = euclidean_distances(numpy_nbrs, [[r[x], r[y]]])
        w = gaussian_transform(w)

        f_ind = 0
        for f in overlay.domain.variables:
            if isinstance(f, ContinuousVariable):
                val1 = overlay[ind, f]
                val2 = overlay[nbr_ind, f].X_df.to_numpy()
                f_score = w * (val1 - val2) ** 2
                score[f_ind, ind] = np.mean(f_score)
                f_ind += 1
        ind += 1

    out = []
    f_ind = 0
    for f in overlay.domain.variables:
        if isinstance(f, ContinuousVariable):
            out.append((f.name, np.mean(score[f_ind, :])))
            f_ind += 1

    return out


