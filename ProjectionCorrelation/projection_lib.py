import math

import Orange.data
import numpy as np
import random
from math import sqrt

from sklearn.preprocessing import normalize, PowerTransformer
from sklearn.manifold import TSNE
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics.pairwise import euclidean_distances

from Orange.data import ContinuousVariable, Variable, Table, table_from_frame
from Orange.preprocess import Normalize

from whstudy import WorldIndicators

T = 1000


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
    normalizer = Normalize()
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

    distances, indices = nbrs.kneighbors(ref_list.X_df)

    # Transformer for Gaussian
    pt = PowerTransformer(method="yeo-johnson")

    # 2D table for scores of features in overlay
    score = np.zeros([n_rows, overlay.X.shape[1]])

    ind = 0
    for r in ref_list:
        nbr_ind = indices[ind]
        r_nbrs = ref_list[nbr_ind]
        numpy_nbrs = r_nbrs.metas_df.loc[:, [x.name, y.name]].to_numpy()
        w = euclidean_distances(numpy_nbrs, [[r[x], r[y]]])
        w = pt.fit_transform(w)

        f_ind = 0
        for f in overlay.domain.variables:
            if isinstance(f, ContinuousVariable):
                val1 = overlay[ind, f]
                val2 = overlay[nbr_ind, f].X_df.to_numpy()
                f_score = w * (val1 - val2)**2
                print(np.mean(f_score))
                score[ind, f_ind] = np.mean(f_score)
                f_ind += 1
        ind += 1

    print(score)

    out = []
    f_ind = 0
    for f in overlay.domain.variables:
        out.append((f.name, np.mean(score[f_ind, :])))

    return out


if __name__ == "__main__":
    handle = WorldIndicators('main', 'biolab')
    indicator_codes = [code for code, _, _, _ in handle.indicators()]
    country_codes = [code for code, _ in handle.countries()]
    random.seed(0)
    indicator_codes = random.sample(indicator_codes, 20)
    country_codes = random.sample(country_codes, 20)

    df = handle.data(country_codes, indicator_codes, 2008)

    reference = df.iloc[:, :10].fillna(0)
    overlay = df.iloc[:, 10:].fillna(0)

    ref_tsne = TSNE(n_components=2, learning_rate='auto', init='random').fit_transform(reference.to_numpy())

    tSNE_X = ContinuousVariable("t-SNE-x")
    tSNE_Y = ContinuousVariable("t-SNE-y")

    overlay_table = table_from_frame(overlay)
    ref_table = table_from_frame(reference)
    ref_table = ref_table.add_column(tSNE_X, ref_tsne[:, 0], to_metas=True)
    ref_table = ref_table.add_column(tSNE_Y, ref_tsne[:, 1], to_metas=True)

    scores = score_projections(ref_table, tSNE_X, tSNE_Y, overlay_table)
    for i in scores:
        print(i)

